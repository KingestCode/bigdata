package com.aura.mazh.data.structure03.link;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 描述： 自定义Link 链表
 
 *  链表的核心概念：链表是一种线性的数据结构，每个节点包含数据和下一个节点的引用两部分
 *
 *  链表的核心操作：
 *
 *  1、在尾部添加元素
 *  2、在头部添加元素
 *  3、查找元素
 *  4、插入元素
 *  5、删除元素
 *  6、计算链表长度
 *  7、链表的遍历
 */
public class MyLink_Single<T> {

    // 链表的长度
    private long length = 0;
    // 链表的头节点
    private Node first = null;
    // 链表的尾节点
    private Node last = null;

    /**
     * 表示一个节点
     * 节点中包含两部分：
     *  1、数据data
     *  2、下一个节点的地址引用next
     */
    private class Node<T>{
        T data;
        Node next;
        public Node(T data){
            this.data = data;
        }
    }

    /**
     * 描述： 在链表的尾部添加一个元素
     */
    public boolean addLast(T data){
        // 如果是第一个元素
        if(length == 0){
            first = new Node(data);
            last = first;
        // 如果不是第一个元素
        }else{
            Node newNode = new Node(data);    // 新节点
            last.next = newNode;              // 更新上个节点的next引用
            last = newNode;                   // 更新last就是当前添加的这个最后的newNode
        }
        length += 1;
        return true;
    }

    /**
     * 描述： 默认在末尾插入元素
     */

    public boolean add(T data){
        return addLast(data);
    }


    /**
     * 描述： 在链表的头部添加一个元素
     */
    public boolean addFirst(T data){
        // 如果是第一个元素
        if(length == 0){
            first = new Node(data);
            last = first;
        // 如果不是第一个元素
        }else{
            Node newNode = new Node(data);      // 新的头节点
            newNode.next = first;               // 更新新的头结点的下一个节点的引用为旧的头节点
            first = newNode;                    // 更新头结点引用
        }
        length += 1;
        return true;
    }


    /**
     * 描述： 求链表的长度
     */
    public long getLength(){
        return length;
    }


    /**
     * 描述： 查询链表是否包含某个元素
     */
    public boolean get(T data){
        // 如果链表为空列表，那么直接返回false
        if(first == null || length == 0) return false;

        // 否则，从第一个节点开始寻找
        Node n = first;
        while(n != null){
            Object currentData = n.data;
            if (currentData.equals(data)){
                return true;
            }
            // 寻找下一个节点
            n = n.next;
        }
        return false;
    }


    /**
     * 描述： 往链表中插入一个元素, insertData插入到existData的后面
     * 链表中的这个existData有可能会有很多个，这个方法默认表示插入到第一个existData的后面，如果有需要
     * 可以再定制插入到指定的第几个existData的后面
     */
    public boolean insertAfter(T existData, T insertData){

        if(existData == null) return addLast(insertData);
        Node n = first;
        while(n != null){
            // 证明找到
            if(n.data.equals(existData)){
                Node third = n.next;    // 截断
                Node newNode = new Node(insertData);    // 插入的节点
                newNode.next = third;   // 截断后的后半部分的头结点接到插入节点
                n.next = newNode;       // 插入节点接到n节点后
                length += 1;
                return true;
            }
            n = n.next;
        }
        // 如果代码执行到这个地方，表示没有找到existData， 所以不进行插入。 如果要默认插入到最后，可以自行指定
        return false;
    }

    /**
     * 描述： 往链表中插入一个元素, insertData插入到existData的前面
     * 链表中的这个existData有可能会有很多个，这个方法默认表示插入到第一个existData的后面，如果有需要
     * 可以再定制插入到指定的第几个existData的后面
     */
    public boolean insertBefore(T existData, T insertData){

        if(existData == null) return addLast(insertData);
        Node n = first;
        Node parentNode = null;
        while(n != null){
            // 证明找到
            if(n.data == existData || n.data.equals(existData)){
                Node newNode = new Node(insertData);    // 新节点
                if(n == first){
                    first = newNode;                    // 更新头结点，因为插入之后就变成了头结点
                    newNode.next = n;                   // 更新插入节点的next为当前节点n

                // 如果不是第一个节点。
                }else{
                    parentNode.next = newNode;
                    newNode.next = n;
                }
                length += 1;
            }
            parentNode = n;
            n = n.next;
        }
        // 如果代码执行到这个地方，表示没有找到existData， 所以不进行插入。 如果要默认插入到最后，可以自行指定
        return false;
    }


    /**
     * 描述： 从链表中删除一个元素
     */
    public boolean remove(T data){
        if(first == null || length == 0) return true;
        Node n = first;
        Node parentNode = null;
        while(n != null){
            // 证明找到
            if(n.data == data || n.data.equals(data)){
                // 如果要删除的元素就是第一个，那么直接变更first节点为他的next节点即可。
                if(n == first){
                    first = first.next;
                // 如果不是第一个，那么直接把当前节点n的父节点的next引用指向当前节点n的next引用
                }else{
                    parentNode.next = n.next;
                }
                length -= 1;
                return true;
            }
            parentNode = n;
            n = n.next;
        }
        return false;
    }


    /**
     * 描述： 链表的遍历
     */
    public Iterator<Object> iterator(){
        if(this.first == null) return null;
        List<Object> tList = new ArrayList<Object>();
        Node n = first;
        while(n != null){
            tList.add(n.data);
            n = n.next;
        }
        return tList.iterator();
    }
}
