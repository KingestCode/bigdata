package com.rox.nio;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class MyClient {

    public static void main(String[] args) throws Exception {

        // buf
        ByteBuffer buf = ByteBuffer.allocate(1024 * 8);

        // 开一个 selector
        Selector sel = Selector.open();

        // 开一个套接字通道
        SocketChannel sc = SocketChannel.open();

        // 下面2种连接没区别
        sc.socket().connect(new InetSocketAddress("localhost", 8888));
//        sc.connect(new InetSocketAddress("localhost", 8888));

        // 配置是否阻塞
        sc.configureBlocking(false);

        // 注册 sel 对象及 关注的 key
        sc.register(sel, SelectionKey.OP_READ);

        // 往 buf 中 put 进数据(一次)
//        buf.put("tom".getBytes());
//        buf.flip();
//        sc.write(buf);
//        buf.clear();

        // 如果想要持续不断的通信, 可以开个分线程 (持续收发消息)
        // 程序是从上往下运行的, 运行到下面没有接受到消息的话会塞住
        // 一旦塞住了, 也就无法继续同服务端通信了
        new Thread(){
            public void run() {
                int i = 0 ;
                // 这里创建一个新的缓冲区专用
                ByteBuffer buf2 = ByteBuffer.allocate(1024 * 8);
                while(true){
                    try {
                        buf2.put(("Tom" +  (i ++)).getBytes());
                        buf2.flip();
                        sc.write(buf2);
                        buf2.clear();
                        Thread.sleep(500);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();

        while (true) {

            // 选择器选择, 接受 服务端返回的 key
            sel.select();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            // 从通道读取到缓冲区
            while (sc.read(buf) > 0) {
                // 读到了数据, 就拍一下板
                buf.flip();
                // 从缓冲区 buf 写入到 ByteArrayOutputStream 流
                baos.write(buf.array(), 0, buf.limit());
                // 写完了就清空 缓冲区buf
                buf.clear();
            }

            // 打印接受到的数据
            System.out.println(new String(baos.toByteArray()));
            baos.close();
            // 清空 selectedKeys的 set
            sel.selectedKeys().clear();
        }
    }


}
