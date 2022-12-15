package io.sophiadata.code;

import java.util.HashMap;
import java.util.Map;

/**
 * (@sophiadata) (@date 2022/12/15 14:53).
 */
public class LeetCode3 {
    // 给定一个字符串 s ，请你找出其中不含有重复字符的 最长子串 的长度。
    public static void main(String[] args) {
        String s = "abcabcbb";
        int result = lengthOfLongestSubstring(s);
        System.out.println("The length of the longest substring without repeating characters is " + result + ".");
    }

    public static int lengthOfLongestSubstring(String s) {
        int maxLength = 0;
        Map<Character, Integer> map = new HashMap<>();
        for (int i = 0, j = 0; j < s.length(); j++) {
            if (map.containsKey(s.charAt(j))) {
                i = Math.max(map.get(s.charAt(j)), i);
            }
            maxLength = Math.max(maxLength, j - i + 1);
            map.put(s.charAt(j), j + 1);
        }
        return maxLength;
    }
}
