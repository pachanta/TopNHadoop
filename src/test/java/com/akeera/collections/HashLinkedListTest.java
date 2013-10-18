package com.akeera.collections;

import com.akeera.collections.ClassicLinkedList;
import com.akeera.collections.Node;
import org.junit.Test;

/**
 * @uthor: pavanachanta
 */
public class HashLinkedListTest {

    @Test
    public void testAddElement(){

        ClassicLinkedList<String> linkedList = new ClassicLinkedList<String>();

        String[] testSeq = new String[]{"X","X","Y","Z"};

        linkedList = addSeq(linkedList,testSeq);
        Node<String> qNode =  linkedList.addAndGetFirst("Q");
        System.out.println(linkedList + "\t" + linkedList.size());
        Node<String> rNode =  linkedList.addAndGetFirst("R");
        System.out.println(linkedList + "\t" + linkedList.size());
        linkedList.insertElementAfterNode(qNode,"F");
        System.out.println(linkedList + "\t" + linkedList.size());
        linkedList.removeNode(qNode);
        System.out.println(linkedList + "\t" + linkedList.size());
        linkedList.removeLast();
        linkedList.removeFirst();
        System.out.println(linkedList + "\t" + linkedList.size());
    }


    private ClassicLinkedList addSeq(ClassicLinkedList<String> hashLinkedList, String[] seq){

        for(String s : seq){
            hashLinkedList.add(s);
        }
        return hashLinkedList;
    }


}
