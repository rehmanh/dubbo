/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.remoting.transport.netty4;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.bytecode.NoSuchMethodException;
import org.apache.dubbo.common.bytecode.Wrapper;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeServer;
import org.apache.dubbo.remoting.exchange.Exchangers;
import org.apache.dubbo.remoting.exchange.support.Replier;
import org.apache.dubbo.remoting.exchange.support.ReplierDispatcher;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;


/**
 * ReplierDispatcherTest
 */

public class ReplierDispatcherTest {

    private ExchangeServer exchangeServer;

    private ConcurrentHashMap<String, ExchangeChannel> clients = new ConcurrentHashMap<>();

    private int port;

    @BeforeEach
    public void startServer() throws RemotingException {
        port = NetUtils.getAvailablePort();
        ReplierDispatcher dispatcher = new ReplierDispatcher();
        Replier<RpcMessage> replierMock = createReplierMock();
        dispatcher.addReplier(RpcMessage.class, replierMock);
        dispatcher.addReplier(Data.class, (channel, msg) -> new StringMessage("hello world"));
        exchangeServer = Exchangers.bind(URL.valueOf("exchange://localhost:" + port + "?" + CommonConstants.TIMEOUT_KEY + "=60000"), dispatcher);
    }

    @Test
    public void testDataPackage() throws Exception {
        ExchangeChannel client = Exchangers.connect(URL.valueOf("exchange://localhost:" + port + "?" + CommonConstants.TIMEOUT_KEY + "=60000"));
        Random random = new Random();
        for (int i = 5; i < 100; i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < i * 100; j++)
                sb.append('(').append(random.nextLong()).append(')');
            Data d = new Data();
            d.setData(sb.toString());
            Assertions.assertEquals(client.request(d).get().toString(), "hello world");
        }
        clients.put(Thread.currentThread().getName(), client);
    }


    @Test
    public void testMultiThread() throws Exception {
        int tc = 10;
        ExecutorService exec = Executors.newFixedThreadPool(tc);
        for (int i = 0; i < tc; i++)
            exec.execute(() -> {
                try {
                    clientExchangeInfo(port);
                } catch (Exception e) {
                    fail();
                }
            });
        exec.shutdown();
        exec.awaitTermination(10, TimeUnit.SECONDS);
    }

    void clientExchangeInfo(int port) throws Exception {
        ExchangeChannel client = Exchangers.connect(URL.valueOf("exchange://localhost:" + port + "?" + CommonConstants.TIMEOUT_KEY + "=5000"));
        clients.put(Thread.currentThread().getName(), client);
        MockResult result = (MockResult) client.request(new RpcMessage(DemoService.class.getName(), "plus", new Class<?>[]{int.class, int.class}, new Object[]{55, 25})).get();
        Assertions.assertEquals(result.getResult(), 80);
        for (int i = 0; i < 100; i++) {
            client.request(new RpcMessage(DemoService.class.getName(), "sayHello", new Class<?>[]{String.class}, new Object[]{"qianlei" + i}));
        }
        for (int i = 0; i < 100; i++) {
            CompletableFuture<Object> future = client.request(new Data());
            Assertions.assertEquals(future.get().toString(), "hello world");
        }
    }


    @AfterEach
    public void tearDown() {
        try {
            if (exchangeServer != null)
                exchangeServer.close();
        } finally {
            if (clients.size() != 0)
                clients.forEach((key, value) -> {
                    value.close();
                    clients.remove(key);
                });
        }
    }

    private Replier<RpcMessage> createReplierMock() throws RemotingException {
        Replier<RpcMessage> replierMock = mock(Replier.class);
        when(replierMock.reply(any(ExchangeChannel.class), any(RpcMessage.class))).thenAnswer((arguments) -> {
            ExchangeChannel channel = arguments.getArgument(0);
            RpcMessage msg = arguments.getArgument(1);
            String desc = msg.getMethodDesc();
            Object[] args = msg.getArguments();
            Object impl = getImplementation(msg.getClassName());
            Wrapper wrap = Wrapper.getWrapper(impl.getClass());
            try {
                return new MockResult(wrap.invokeMethod(impl, desc, msg.getParameterTypes(), args));
            } catch (NoSuchMethodException e) {
                throw new RemotingException(channel, "Service method not found.");
            } catch (InvocationTargetException e) {
                return new MockResult(e.getTargetException());
            }
        });
        return replierMock;
    }

    private Object getImplementation(String service) {
        String impl = service + "Impl";
        try {
            Class<?> cl = Thread.currentThread().getContextClassLoader().loadClass(impl);
            return cl.newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    static class Data implements Serializable {
        private static final long serialVersionUID = -4666580993978548778L;

        private String mData = "";

        public Data() {
        }

        public String getData() {
            return mData;
        }

        public void setData(String data) {
            mData = data;
        }
    }

    static class StringMessage implements Serializable {
        private static final long serialVersionUID = 7193122183120113947L;

        private String mText;

        StringMessage(String msg) {
            mText = msg;
        }

        public String toString() {
            return mText;
        }
    }
}
