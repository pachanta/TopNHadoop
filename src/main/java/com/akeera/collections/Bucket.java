package com.akeera.collections;

import com.akeera.collections.ClassicLinkedList;
import com.akeera.collections.Node;

public class Bucket<E>{
    ClassicLinkedList<E> elements;

    public int count;

    public Bucket(int count){
        this.count = count;
        elements = new ClassicLinkedList<E>();
    }

    public int size(){
        return elements.size();
    }

    public String toString(){

        StringBuilder sb = new StringBuilder();

        sb.append("Bucket(").append(count).append("):[");
        if(this.elements != null && this.elements.size() > 0){
            Node<E> eNode = elements.getFirst();
            while(eNode != null){
                sb.append(eNode.item.toString()).append(",");
                eNode = eNode.next;
            }
        }
        sb.append("]");

        return sb.toString();

    }
}
