package com.aura.mazh.data.structure03.link;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 描述： 双向链表
 */
public class MyLink_Double<T> {

    // 链表的长度
    private long length = 0;
    // 链表的头节点
    private Node first = null;
    // 链表的尾节点
    private Node last = null;

    /**
     * 表示一个节点
     * 节点中包含两部分：
     *  1、数据
     *  2、下一个节点的地址引用next
     *  3、上一个节点的地址引用before
     */
    private class Node<T>{
        T data;
        Node next;
        Node before;
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
            newNode.before = last;            // 更新上个节点的before引用为之前的最后一个节点
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

            first.before = newNode;             // 原来的first节点变成了第二个节点，所以第二个节点的before应用是变成新的first的newNode
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
     * 描述： 查询链表是否包含某个元素, 顺着找
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

        if(existData == null) {    // 如果不指定existData，则默认插入到链表的最后
            return addLast(insertData);
        }

        Node n = first;
        while(n != null){
            // 证明找到
            if(n.data.equals(existData)){
                // 节点1和节点3截断， 中间插入节点2
                // 此时，n就是节点1(one)， third就是节点3(third)， newNode就是节点2(two)
                Node one = n;
                Node two = new Node(insertData);
                Node third = n.next;

                two.before = one;
                one.next = two;

                // 如果是插入最后一个元素的后面。那么根本就没有third
                if(third != null){      // 有third
                    two.next = third;
                    third.before = two;
                }else{                  // 没有third
                    last = two;
                }

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
                // 节点1和节点3截断， 中间插入节点2
                // 此时，n就是节点1(parentNode)， third就是节点3(n)， newNode就是节点2(newNode)
                Node newNode = new Node(insertData);    // 新节点

                Node one = parentNode;
                Node two = newNode;
                Node three = n;

                if(n == first){                         // 则没有one
                    newNode.next = first;               // 更新插入节点的next为当前节点n
                    first.before = newNode;
                    first = newNode;                    // 更新头结点，因为插入之后就变成了头结点

                // 如果不是第一个节点。
                }else{
                    two.next = three;
                    two.before = one;

                    one.next = two;
                    three.before = two;
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

        // 如果删除的是 first
        if(first == data || first.data.equals(data)){
            Node next = first.next;
            next.before = null;
            first = next;
        // 如果删除的是 last
        }else if(last == data || last.data.equals(data)){
            Node newLast = last.before;
            newLast.next = null;
            last = newLast;
        }else{
            Node parentNode = null;
            Node n = first;
            while(n != null){
                parentNode = n.before;
                if(n == data || n.data.equals(data)){
                    Node next = n.next;
                    parentNode.next = next;
                    next.before = parentNode;
                }
                n = n.next;
            }
        }

        length = length == 0 ? 0 : length - 1;
        return true;
    }


    /**
     * 描述： 链表的遍历
     */
    public Iterator<Object> iterator(){
        if(first == null) return null;
        List<Object> tList = new ArrayList<Object>();
        Node n = first;
        while(n != null){
            tList.add(n.data);
            n = n.next;
        }
        return tList.iterator();
    }

    /**
     * 描述： 默认order是false,那么就调用iterator();  否则逆序遍历
     * 也就是说： 如果
     *
     *  order == false,  那么  first ----> last
     *  order == true,   那么  last ----> first
     */
    public Iterator<Object> iterator(boolean order){
        if(order){
            if(this.last == null) return null;
            List<Object> tList = new ArrayList<Object>();
            Node n = last;
            while(n != null){
                tList.add(n.data);
                n = n.before;
            }
            return tList.iterator();
        }else{
            return iterator();
        }
    }

}
