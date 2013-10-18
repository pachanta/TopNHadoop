package com.akeera.collections;

import java.util.Hashtable;
import java.util.Iterator;

/**
 * @author: pavanachanta
 */
public class SpaceSavingTopN<E>{


    ClassicLinkedList<Bucket> buckets;

    Hashtable<E,Node<E>> elementIndex;

    //number of counters to maintain
    private int m;

    public SpaceSavingTopN(int m) {
        buckets = new ClassicLinkedList<Bucket>();
        elementIndex = new  Hashtable<E,Node<E>>();
        if(m < 1){
            m = 1;
        }
        this.m = m;
    }


    public int getCountByElement(E e){
        if(elementIndex.containsKey(e))
            return ((Bucket)elementIndex.get(e).parent.item).count;
        else
            return 0;
    }



    /**
     * Adds an element to the topN list.
     *
     */
    public boolean add(E e) {

        if(elementIndex.containsKey(e)){

            Node<E> elementNode =  elementIndex.get(e);
            Node<Bucket> bucketNode = elementNode.parent;
            int count = bucketNode.item.count;
            count++;

            //If the next higher frequency node count matches that of freq of current item to be inserted
            if(bucketNode.next != null){

                //take the element out of current bucket
                bucketNode.item.elements.unlink(elementNode);

                if(bucketNode.next.item.count == count){
                    bucketNode.next.item.elements.addNodeFirst(elementNode);
                    elementNode.parent = bucketNode.next;
                }else{
                    Bucket<E> b = new Bucket<E>(count);
                    b.elements.addNodeFirst(elementNode);
                    Node<Bucket> newBucketNode = buckets.insertElementAfterNode(bucketNode,b);
                    elementNode.parent = newBucketNode;
                }

                if(bucketNode.item.elements.size() == 0){
                    buckets.unlink(bucketNode);
                }
            }else{
                bucketNode.item.elements.unlink(elementNode);

                Bucket<E> b = new Bucket<E>(count);
                b.elements.addNodeFirst(elementNode);
                Node<Bucket> newBucketNode = buckets.insertElementAfterNode(bucketNode,b);
                elementNode.parent = newBucketNode;
                if(bucketNode.item.elements.size() == 0){
                    buckets.unlink(bucketNode);
                }
            }

        }else{

            //if we reached the limit of counters m, then replace the element with minimum count
            if(size() == m){
                //if there are multiple elements in the bucket then remove the oldest(last or LRU)
                Object element = buckets.getFirst().item.elements.removeLast();
                elementIndex.remove(element);

                if(buckets.getFirst().item.elements.size() == 0){
                    buckets.removeFirst();
                }
            }

            //if there is a bucket for count "1" then use it.
            //else create one.
            if(!buckets.isEmpty() && buckets.getFirst().item.count == 1){
                Node<E> elementNode =   buckets.getFirst().item.elements.addAndGetFirst(e);
                elementNode.parent  =   buckets.getFirst();
                elementIndex.put(e,elementNode);

            }else{
                Bucket<E> b = new Bucket<E>(1);
                Node<E> elementNode = b.elements.addAndGetFirst(e);
                Node<Bucket> bucketNode = buckets.addAndGetFirst(b);
                elementNode.parent = bucketNode;
                elementIndex.put(e,elementNode);
            }

        }

        return true;
    }


    public int size(){
        return elementIndex.size();
    }

    public Iterator<E> iterator(E e){
        return null;
    }

    private void removeNode(Node n){

    }

    public String toString(){

        StringBuilder sb = new StringBuilder();

        sb.append("SpaceSavingTopN[\n");

        if(this.buckets != null && this.buckets.size() > 0){
            Node<Bucket> eNode = buckets.getFirst();
            while(eNode != null){
                sb.append(eNode.item.toString()).append(",\n");
                eNode = eNode.next;
            }
        }
        sb.append("]");

        return sb.toString();

    }

    public Hashtable<E, Node<E>> getElementIndex() {
        return elementIndex;
    }


    public ClassicLinkedList<Bucket> getBuckets() {
        return buckets;
    }

}
