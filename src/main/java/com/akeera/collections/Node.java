package com.akeera.collections;


public class Node<E> {
    public E item;
    public Node<E> next;
    public Node<E> prev;
    public Node parent;

    public Node(Node<E> prev, E element, Node<E> next) {
        this.item = element;
        this.next = next;
        this.prev = prev;
    }

    public String toString(){

        return "Node(" + item.toString() + ")";

    }
}