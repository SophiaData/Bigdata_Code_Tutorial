package io.sophiadata.code;

import java.util.HashMap;
import java.util.Map;

/** (@sophiadata) (@date 2022/12/15 14:53). */
public class LeetCode1 {
  // 给定一个整数数组 nums 和一个整数目标值 target，请你在该数组中找出 和为目标值 target 的那 两个 整数，并返回它们的数组下标。
  // 你可以假设每种输入只会对应一个答案。但是，数组中同一个元素在答案里不能重复出现。
  // 你可以按任意顺序返回答案。
  public static void main(String[] args) {
    int[] nums = { 2, 7, 11, 15 };
    int target = 9;
    int[] result = twoSum(nums, target);
    System.out.println(
        "The indices of the two numbers that add up to the target are " + result[0] + " and " + result[1] + ".");
  }

  public static int[] twoSum(int[] nums, int target) {
    Map<Integer, Integer> map = new HashMap<>();
    for (int i = 0; i < nums.length; i++) {
      int complement = target - nums[i];
      if (map.containsKey(complement)) {
        return new int[] { map.get(complement), i };
      }
      map.put(nums[i], i);
    }
    throw new IllegalArgumentException("No two sum solution");
  }
}