package com.aura.mazh.data.structure03.link;

import java.util.Iterator;

/**
 * 描述： 测试链表
 */
public class LinkTest {

    public static void main(String[] args) {
        testLinkSingle();
        System.out.println("-----------------------");
        testLinkDouble();
    }
    public static void iterator(MyLink_Single linkSingle){
        Iterator<Object> iterator = linkSingle.iterator();
        while(iterator.hasNext()){
            Object next = iterator.next();
            System.out.print(next+"\t");
        }
        System.out.println();
    }


    public static void iterator(MyLink_Double linkDouble, boolean order){
        Iterator<Object> iterator = null;
        if(order){
            iterator = linkDouble.iterator(true);
        }else{
            iterator = linkDouble.iterator();
        }
        while(iterator.hasNext()){
            Object next = iterator.next();
            System.out.print(next+"\t");
        }
        System.out.println();
    }


    public static void testLinkDouble(){

        MyLink_Double<Integer> linkDouble = new MyLink_Double<Integer>();


        linkDouble.addFirst(2);
        linkDouble.addLast(3);
        linkDouble.addFirst(1);
        linkDouble.addFirst(4);
        linkDouble.addLast(5);
        linkDouble.add(6);
        linkDouble.insertAfter(2,22);
        linkDouble.insertBefore(2,222);
        System.out.println("currentlength : " + linkDouble.getLength());
        iterator(linkDouble, false);
        iterator(linkDouble, true);


        linkDouble.remove(4);
        linkDouble.remove(2);
        System.out.println("currentlength : " + linkDouble.getLength());
        iterator(linkDouble, false);
        iterator(linkDouble, true);


        linkDouble.insertAfter(6, 33);
        linkDouble.addLast(444);
        System.out.println("currentlength : " + linkDouble.getLength());
        iterator(linkDouble, false);
        iterator(linkDouble, true);
    }


    public static void testLinkSingle(){
        MyLink_Single<String> linkSingle = new MyLink_Single<String>();
        // 结果： 空

        linkSingle.add("huangbo");
        // 结果： huangbo
        iterator(linkSingle);

        linkSingle.add("xuzheng");
        // 结果： huangbo xuzheng
        iterator(linkSingle);

        linkSingle.addFirst("wangbaoqiang");
        // 结果： wangbaoqiang huangbo xuzheng
        iterator(linkSingle);

        linkSingle.insertBefore("wangbaoqiang","liuyifei");
        // 结果： liuyifei wangbaoqiang huangbo xuzheng
        iterator(linkSingle);

        linkSingle.insertAfter("wangbaoqiang", "liujialing");
        // 结果： liuyifei wangbaoqiang liujialing huangbo xuzheng
        iterator(linkSingle);

        System.out.println(linkSingle.getLength());
        System.out.println(linkSingle.get("xuzheng"));

        linkSingle.remove("huangbo");
        // 结果： liuyifei wangbaoqiang liujialing xuzheng
        iterator(linkSingle);
        System.out.println(linkSingle.getLength());
        System.out.println(linkSingle.get("huangbo"));
    }
}
