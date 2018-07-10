package com.rox.LCS;

public class Longest_Common_Subsequence {

    public int lengthOfLCS(String A, String B) {

        int n = A.length();
        int m = B.length();
        char[] a = A.toCharArray();
        char[] b = B.toCharArray();

        int[][] dp = new int[n][m];

        // 第一列
        for (int i = 0; i < n; i++) {
            if (a[i] == b[0]) {
                dp[i][0] = 1;
                for (int j = i + 1; j < n; j++) {
                    dp[j][0] = 1;
                }
                break;
            }
        }

        // 第一行
        for (int i = 0; i < m; i++) {
            if (a[0] == b[i]) {
                dp[0][i] = 1;
                for (int j = i + 1; j < m; j++) {
                    dp[0][j] = 1;
                }
                break;
            }
        }

        // 其它行和列(双重 for 循环, 从1开始, 套公式)
        for (int i = 1; i < n; i++) {
            for (int j = 1; j < m; j++) {
                if (a[i] == b[j]) {
                    dp[i][j] = dp[i - 1][j - 1] + 1;
                } else {
                    dp[i][j] = Math.max(dp[i][j-1],dp[i-1][j]);
                }
            }
        }


        // 以上已经把所有的表中元素都求出来了, 下面打印一下
        for(int i = 0;i<n;i++){
            for(int j = 0;j<m;j++){
                System.out.print(dp[i][j]+"\t");
            }
            System.out.println();
        }


        return dp[n-1][m-1];
    }


    public static void main(String[] args) {
        Longest_Common_Subsequence lcs = new Longest_Common_Subsequence();
        int len = lcs.lengthOfLCS("androidm","random");
        System.out.println("\n最大公共字符串数量为: "+len);
    }
}


/**
 *      矩阵图如下
 *
 *         r    a   n   d   o   m
        a  0	1	1	1	1	1
        n  0	1	2	2	2	2
        d  0	1	2	3	3	3
        r  1	1	2	3	3	3
        o  1	1	2	3	4	4
        i  1	1	2	3	4	4
        d  1	1	2	3	4	4
        m  1	1	2	3	4	5

 最大公共字符串数量为: 5



 公式如下:

 公式:

 C[i,j] =
 * 0        若 i=0 或 j=0
 * C[i-1,  j-1] + 1      若 i, j > 0,  x[ i ] = y[ j ]
 * 左上斜 的数 + 1
 * max{ C[ i, j-1 ],  C[ i -1, j ] }  若 i, j > 0,  x[ i ] != y[ j ]
 * 左边 || 上面 的数 , 取最大值.

 */

