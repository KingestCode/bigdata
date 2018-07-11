package com.rox.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;

/**
 * 注意: selector挑选器, 维护若干集合
 * selectionKeys : 注册的key
 * selectedKeys  : 挑选出来的key -- 有事件的 key
 * 客户端 & 服务端对应的客户端 SocketChannel(套接字通道), 一般来讲, 只需要对 read 感兴趣
 * 也就是拦截 read 消息, write 的话随时都可以 write
 * sc 是可读可写的
 */
public class MyServer {

    public static void main(String[] args) throws Exception {

        // 分配缓冲区buf内存
        ByteBuffer buf = ByteBuffer.allocate(1024);

        // 开启挑选器
        Selector sel = Selector.open();

        // 开启 ServerSocket 通道
        ServerSocketChannel ssc = ServerSocketChannel.open();
        // 绑定端口
        ssc.socket().bind(new InetSocketAddress("localhost", 8888));


        // 设置非阻塞
        ssc.configureBlocking(false);

           // 在挑选器中注册通道(服务器通道, 和感兴趣的事件 - OP_ACCEPT)
        ssc.register(sel, SelectionKey.OP_ACCEPT);

        // 初始化可选择通道对象(为 ServerSocketChannel & SocketChannel 的共同父类)
        SelectableChannel sc = null;

        while (true) {
            // 挑选器开始选择(阻塞的)
            // 如果么有接收到 感兴趣事件, 就塞在这里
            sel.select();

            // 能走到这一步, 就是已经接收到了 accept 事件
            // 迭代挑选出来的 key 的集合
            Iterator<SelectionKey> it = sel.selectedKeys().iterator();
            while (it.hasNext()) {
                SelectionKey key = it.next();

                try {
                    // 如果注册的 key是可接受的, 就一定是服务器通道
                    if (key.isAcceptable()) {

                        // 取出该通道, 返回一个 SelectableChannel 的超类对象
                        sc = key.channel();

                        // 因为拿到的 key 是 isAcceptable, 所以可以判断是 ssc 对象, 强转
                        // 并通过 accept()方法, 返回一个 sc 对象(类似于套接字, 与客户端的 sc 对应, 负责跟客户端的 sc 通信)
                        SocketChannel sc0 = ((ServerSocketChannel) sc).accept();

                        // 设置 sc0 为非阻塞
                        sc0.configureBlocking(false);

                        // 为接收到的这个 sc 在选择器中, 注册读事件
                        sc0.register(sel, SelectionKey.OP_READ);
                    }

                    // 下一次轮询, 发现是来自 读事件 的key
                    if (key.isReadable()) {
                        // 取出 channel, 直接强转为 SocketChannel
                        SocketChannel sc1 = (SocketChannel) key.channel();

                        /// 回复客户端消息, 前面加个头'hello', 然后再写回去
                        // 创建消息字节数组
                        byte[] helloBytes = "hello: ".getBytes();
                        // 把字节数组放入 buf
                        buf.put(helloBytes, 0, helloBytes.length);

                        // 读取消息
                        // 不存在读完, 只要不为0, 就一直轮询读
                        // 从通道里读出来,放到缓冲区里
                        while (sc1.read(buf) > 0) {
                            // 拍板, 定稿, > 0说明有数据
                            // position 归0, limit 置在 已写元素 的后面一格, 此时不接受其它写入了
                            buf.flip();
                            // 持有的 sc 对象写入到channel
                            sc1.write(buf);
                            // 写完后 清空, position 归0, limit 最大
                            buf.clear();
                        }
                    }
                } catch (Exception e) {
                    // 如果失败, 移除本次接收到的 keys
                    sel.keys().remove(key);
                }
            }

            // 本次选择器事件处理完了之后
            // 移除所有选择出来的 key
            sel.selectedKeys().clear();
        }

    }





}
