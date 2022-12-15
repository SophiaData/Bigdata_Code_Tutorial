package io.sophiadata.code_tutorial;

/** (@sophiadata) (@date 2022/12/15 14:53). */
public class LeetCode2 {
    // 给你两个 非空 的链表，表示两个非负的整数。
    // 它们每位数字都是按照 逆序 的方式存储的，并且每个节点只能存储 一位 数字。
    // 请你将两个数相加，并以相同形式返回一个表示和的链表。
    // 你可以假设除了数字 0 之外，这两个数都不会以 0 开头。
    public static void main(String[] args) {
        ListNode l1 = new ListNode(2);
        l1.next = new ListNode(4);
        l1.next.next = new ListNode(3);

        ListNode l2 = new ListNode(5);
        l2.next = new ListNode(6);
        l2.next.next = new ListNode(4);

        ListNode result = addTwoNumbers(l1, l2);
        System.out.println(
                "The result of adding the two numbers is "
                        + result.val
                        + " -> "
                        + result.next.val
                        + " -> "
                        + result.next.next.val
                        + " -> null");
    }

    public static ListNode addTwoNumbers(ListNode l1, ListNode l2) {
        ListNode dummy = new ListNode(0);
        ListNode current = dummy;
        int carry = 0;
        while (l1 != null || l2 != null) {
            int x = (l1 != null) ? l1.val : 0;
            int y = (l2 != null) ? l2.val : 0;
            int sum = x + y + carry;
            carry = sum / 10;
            current.next = new ListNode(sum % 10);
            current = current.next;
            if (l1 != null) {
                l1 = l1.next;
            }
            if (l2 != null) {
                l2 = l2.next;
            }
        }
        if (carry > 0) {
            current.next = new ListNode(carry);
        }
        return dummy.next;
    }
}

class ListNode {
    int val;
    ListNode next;

    ListNode(int x) {
        val = x;
    }
}



