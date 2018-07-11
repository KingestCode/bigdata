package com.rox.eight_queen;

public class EightQueue {

    public static int num = 0; //累计方案
    public static final int MAXQUEEN = 8;
    public static int[] cols = new int[MAXQUEEN];


    public void getCount(int n) {
        // 记录每列每个方格是否可以放皇后, 每列中的每个数用 rows[x] 标识
        boolean[] rows = new boolean[MAXQUEEN];

        // n是指从哪一列开始遍历,查找能放皇后的 row号, 是自己传进来的值, 后面会++
        // m是指 放皇后的 地方
        for (int m = 0; m < n; m++) {
            // m 对应的列的行(二维表定位到 m),的此行(整个一行)都不能再放皇后了
            rows[cols[m]] = true;

            // 列 col 之间的差值
            int d = n - m;

            // 注意: 第一个格子是第0列, 第二个格子是第1列..
            // 如果是正斜方向  :TODO 待完全验证
            // 好像 col[m] >= d 时候, 是正斜(右上), 可在多个格子验证, 已经验证了2个
            if (cols[m] - d >= 0) {
                // 这个格子不能放
                rows[cols[m] - d] = true;
            }

            // 如果是反斜方向
            if (cols[m] + d <= (MAXQUEEN - 1)) {
                // 同样也不能放
                rows[cols[m] + d] = true;
            }
        }

        // 到此知道了哪些位置不能放皇后
        for (int i = 0; i < MAXQUEEN; i++) {

            if (rows[i]) {
                //不能放, 加入 rows 数组中的都是不能放的
                continue;
            }

            // 剩下的都是能放的, i是值得 rows[i], 第 i 行
            // 从 n 行开始遍历寻找能放皇后位置的结果为 i
            cols[n] = i;

            // 如果当前还没遍历完 最大的皇后数 对应的列数
            if (n < MAXQUEEN - 1) {
                // 递归调用自己, 遍历寻找后面一列的皇后
                getCount(n + 1);
            } else {
                // 找完了最大皇后数, 所对应的所有列
                // 找到完整的一套方案
                // 累计方案数 ++
                num++;

                // 打印出当前的矩阵排列
                printQueen();
            }
            //下面可能仍然有合法位置

        }
    }

    private void printQueen() {
        System.out.println("第"+num+"种方案");
        for(int i = 0;i<MAXQUEEN;i++){
            for(int j = 0;j<MAXQUEEN;j++){
                if(i == cols[j]){
                    System.out.print("0 ");
                }else{
                    System.out.print("+ ");
                }
            }
            System.out.println();
        }
    }

    public static void main(String[] args) {

        EightQueue queen = new EightQueue();
        queen.getCount(0);
    }

    /**
     * 结果: 共92种
     *
     * 第1种方案
     0 + + + + + + +
     + + + + + + 0 +
     + + + + 0 + + +
     + + + + + + + 0
     + 0 + + + + + +
     + + + 0 + + + +
     + + + + + 0 + +
     + + 0 + + + + +
     第2种方案
     0 + + + + + + +
     + + + + + + 0 +
     + + + 0 + + + +
     + + + + + 0 + +
     + + + + + + + 0
     + 0 + + + + + +
     + + + + 0 + + +
     + + 0 + + + + +

     .....
     */

}

