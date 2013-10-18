package com.akeera.collections;

import java.util.*;

/**

 */

public class ClassicLinkedList<E>

{
    transient int size = 0;

    /**
     * Pointer to first node.
     * Invariant: (first == null && last == null) ||
     *            (first.prev == null && first.item != null)
     */
    transient Node<E> first;

    /**
     * Pointer to last node.
     * Invariant: (first == null && last == null) ||
     *            (last.next == null && last.item != null)
     */
    transient Node<E> last;

    /**
     * Constructs an empty list.
     */
    public ClassicLinkedList() {
    }


    /**
     * Links e as first element.
     */
    private Node<E> linkFirst(E e) {
        Node<E> f = first;
        Node<E> newNode = new Node<E>(null, e, f);
        first = newNode;
        if (f == null)
            last = newNode;
        else
            f.prev = newNode;
        size++;
        
        return newNode;
    }



    /**
     * Links e as first element.
     */
    public Node<E> insertElementAfterNode(Node<E> node,E e) {

        Node<E> newNode = new Node<E>(node, e, node.next);

        if (node.next != null)
            node.next.prev = newNode;
        else
            last = newNode;

        node.next = newNode;
        size++;

        return   newNode;
    }


    /**
     * Links e as first element.
     */
    public void addNodeFirst(Node<E> node) {
        final Node<E> f = first;
        node.next = f;
        first = node;
        if (f == null)
            last = node;
        else
            f.prev = node;
        size++;
        
    }

    /**
     * Links e as last element.
     */
    void linkLast(E e) {
        final Node<E> l = last;
        final Node<E> newNode = new Node<E>(l, e, null);
        last = newNode;
        if (l == null)
            first = newNode;
        else
            l.next = newNode;
        size++;
        
    }

    /**
     * Inserts element e before non-null Node succ.
     */
    public void linkBefore(E e, Node<E> succ) {
        // assert succ != null;
        final Node<E> pred = succ.prev;
        final Node<E> newNode = new Node<E>(pred, e, succ);
        succ.prev = newNode;
        if (pred == null)
            first = newNode;
        else
            pred.next = newNode;
        size++;
        
    }

    /**
     * Unlinks non-null first node f.
     */
    private E unlinkFirst(Node<E> f) {
        // assert f == first && f != null;
        final E element = f.item;
        final Node<E> next = f.next;

        f.next = null; // help GC
        first = next;
        if (next == null)
            last = null;
        else
            next.prev = null;
        size--;
        
        return element;
    }

    /**
     * Unlinks non-null last node l.
     */
    private E unlinkLast(Node<E> l) {
        // assert l == last && l != null;
        final E element = l.item;
        final Node<E> prev = l.prev;

        l.prev = null; // help GC
        last = prev;
        if (prev == null)
            first = null;
        else
            prev.next = null;
        size--;
        
        return element;
    }

    /**
     * Unlinks non-null node x.
     */
    public E unlink(Node<E> x) {
        // assert x != null;
        final E element = x.item;
        final Node<E> next = x.next;
        final Node<E> prev = x.prev;

        if (prev == null) {
            first = next;
        } else {
            prev.next = next;
            x.prev = null;
        }

        if (next == null) {
            last = prev;
        } else {
            next.prev = prev;
            x.next = null;
        }

        size--;
        
        return element;
    }

    /**
     * Returns the first element in this list.
     *
     * @return the first element in this list
     * @throws NoSuchElementException if this list is empty
     */
    public Node<E> getFirst() {

        return first;

    }

    /**
     * Returns the last element in this list.
     *
     * @return the last element in this list
     * @throws NoSuchElementException if this list is empty
     */
    public Node<E> getLast() {
        return last;
    }

    /**
     * Removes and returns the first element from this list.
     *
     * @return the first element from this list
     * @throws NoSuchElementException if this list is empty
     */
    public E removeFirst() {
        final Node<E> f = first;
        if (f == null)
            throw new NoSuchElementException();
        return unlinkFirst(f);
    }



    /**
     * Removes and returns the first element from this list.
     *
     * @return the first element from this list
     * @throws NoSuchElementException if this list is empty
     */
    public Node<E> removeNode(Node<E> node) {

        if(node.prev == null ){
            if(node.next == null){
                first = last = null;
            }else{
                first = node.next;
                first.prev = null;
            }
        }else{
            if(node.next == null){
                last = node.prev;
                node.prev.next = null;
            }else{
                node.prev.next = node.next;
                node.next.prev = node.prev;
            }
        }
        size--;
        return node;

    }



    /**
     * Removes and returns the last element from this list.
     *
     * @return the last element from this list
     * @throws NoSuchElementException if this list is empty
     */
    public E removeLast() {
        final Node<E> l = last;
        if (l == null)
            throw new NoSuchElementException();
        return unlinkLast(l);
    }

    /**
     * Inserts the specified element at the beginning of this list.
     *
     * @param e the element to add
     */
    public void addFirst(E e) {
        linkFirst(e);
    }

    /**
     * Inserts the specified element at the beginning of this list.
     *
     * @param e the element to add
     */
    public Node<E> addAndGetFirst(E e) {
        return linkFirst(e);
    }



    /**
     * Returns the number of elements in this list.
     *
     * @return the number of elements in this list
     */
    public int size() {
        return size;
    }

    /**
     * Appends the specified element to the end of this list.
     *
     *
     * @param e element to be appended to this list
     * @return {@code true} (as specified by {@link Collection#add})
     */
    public boolean add(E e) {
        linkLast(e);
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation returns <tt>size() == 0</tt>.
     */
    public boolean isEmpty() {
        return size() == 0;
    }


    public String toString(){

        StringBuilder sb = new StringBuilder();
        Node<E> node = first;
        sb.append("[");
        while(node != null){
            sb.append(node.item.toString()).append(",");
            node = node.next;
        }
        sb.append("]");

        return sb.toString();
    }



}
