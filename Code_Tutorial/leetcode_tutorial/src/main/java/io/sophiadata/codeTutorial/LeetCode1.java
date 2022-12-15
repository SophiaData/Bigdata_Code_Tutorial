package io.sophiadata.codeTutorial;

import java.util.HashMap;
import java.util.Map;

public class LeetCode1 {
    public static void main(String[] args) {
      int[] nums = {2, 7, 11, 15};
      int target = 9;
      int[] result = twoSum(nums, target);
      System.out.println("The indices of the two numbers that add up to the target are " + result[0] + " and " + result[1] + ".");
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