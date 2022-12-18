package io.sophiadata.code;

import java.util.Arrays;
import java.util.HashMap;

/** (@sophiadata) (@date 2022/12/16 13:52). */
public class LeetCode1 {
    public static void main(String[] args) {
        // 给定一个整数数组 nums 和一个整数目标值 target，
        // 请你在该数组中找出 和为目标值 target  的那 两个 整数，并返回它们的数组下标。
        //
        // 你可以假设每种输入只会对应一个答案。但是，数组中同一个元素在答案里不能重复出现。
        //
        // 你可以按任意顺序返回答案。
        int[] towSum = new LeetCode1().towSum(new int[] {2, 7, 11, 15}, 9);
        System.out.println("两数之和之数组下标为：" + Arrays.toString(towSum));
    }

    public int[] towSum(int[] nums, int target) {
        HashMap<Integer, Integer> hashMap = new HashMap<>();
        int[] result = new int[2];
        for (int i = 0; i < nums.length; i++) {
            int res = target - nums[i];
            Integer j = hashMap.get(res);
            if (j != null) {
                result[0] = i;
                result[1] = j;
                break;
            }
            hashMap.put(nums[i], i);
        }
        return result;
    }
}
