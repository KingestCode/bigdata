package com.rox.spark.java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class Transfromation_Java_变换算子全 {


    /**
     *
     */
    public static void repartitionAndSortWithinPartitions() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("repartitionAndSortWithinPartitions");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        final Random random = new Random();
        JavaPairRDD<Integer, Integer> mapToPair = listRDD.mapToPair(new PairFunction<Integer, Integer, Integer>() {

            public Tuple2<Integer, Integer> call(Integer t) throws Exception {

                return new Tuple2<Integer, Integer>(t, random.nextInt(10));
            }
        });

        JavaPairRDD<Integer, Integer> repartitionAndSortWithinPartitions = mapToPair.repartitionAndSortWithinPartitions(new Partitioner() {

            @Override
            public int numPartitions() {
                return 2;
            }

            @Override
            public int getPartition(Object key) {

                System.out.println("key>>" + key.toString() + " : " + "pati>>" + key.toString().hashCode() % 2);
                return key.toString().hashCode() % 2;
            }
        });

        repartitionAndSortWithinPartitions.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {

            public void call(Tuple2<Integer, Integer> t) throws Exception {
                // TODO Auto-generated method stub
                System.out.println(t._1 + "  :  " + t._2);
            }
        });

    }

    /**
     * mapPartitionsWithIndex与mapPartitions基本相同，只是在处理函数的参数是一个二元元组，元组的第一个元素是当前正在处理的分区的index，元组的第二个元素是当前处理的分区元素组成的Iterator  (可以打印出当前正在处理哪个分区)
     */
    public static void mapPartitionsWithIndex() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("mapPartitionsWithIndex");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> listRDD = sc.parallelize(list, 2);
        listRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            /**
             * index 就是分区的索引
             */
            public Iterator<String> call(Integer index, Iterator<Integer> iterator)
                    throws Exception {
                ArrayList<String> list = new ArrayList();
                while (iterator.hasNext()) {
                    String result = iterator.next() + "_" + index;
                    list.add(result);
                }

                return list.iterator();
            }
        }, true)
                .foreach(new VoidFunction<String>() {

                    public void call(String t) throws Exception {
                        // TODO Auto-generated method stub
                        System.out.println(t);
                    }
                });
    }

    /**
     * 做一个单词计数的
     */

    public static void aggregateByKey() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //如果是在本地运行那么设置setmaster参数为local
        //如果不设置，默认就在集群模式下运行。
        conf.setMaster("local");
        //给任务设置一下名称。
        conf.setAppName("aggregateByKey");
        // ctrl + alt + o
        //创建好了程序的入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        //模拟一个集合，使用并行化的方式创建出来一个RDD。
        List<String> list = Arrays.asList("you	jump", "i	jump");
        JavaRDD<String> listRDD = sc.parallelize(list);
        //U代表的是FlatMapFunction 这个函数的返回值
        listRDD.flatMap(new FlatMapFunction<String, String>() {

            public Iterator<String> call(String t) throws Exception {
                // TODO Auto-generated method stub
                return Arrays.asList(t.split("\t")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {

            public Tuple2<String, Integer> call(String t) throws Exception {

                return new Tuple2<String, Integer>(t, 1);
            }

        })
                /**
                 * 其实reduceBykey就是aggregateByKey的简化版。 就是aggregateByKey多提供了一个函数
                 * 类似于Mapreduce的combine操作（就在map端执行reduce的操作）
                 *
                 * 第一个参数代表的是每个key的初始值初始值：
                 * 第二个是一个函数，类似于map-side的本地聚合
                 * 第三个也是饿函数，类似于reduce的全局聚合
                 */
                .aggregateByKey(0, new Function2<Integer, Integer, Integer>() {

                    public Integer call(Integer v1, Integer v2) throws Exception {
                        // TODO Auto-generated method stub
                        return v1 + v2;
                    }
                }, new Function2<Integer, Integer, Integer>() {

                    public Integer call(Integer v1, Integer v2) throws Exception {
                        // TODO Auto-generated method stub
                        return v1 + v2;
                    }
                }).collect()
                .forEach(new Consumer<Tuple2<String, Integer>>() {
            public void accept(Tuple2<String, Integer> t) {
                System.out.println(t._1 + ":" + t._2);
            }
        });


    }

    /**
     * 随机采样
     */
    public static void sample() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("sample");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        /**
         * 对RDD中的集合内元素进行采样，第一个参数withReplacement是true表示有放回取样，false表示无放回。第二个参数表示比例
         */
        listRDD.sample(true, 0.1)
                .foreach(new VoidFunction<Integer>() {

                    public void call(Integer t) throws Exception {
                        // TODO Auto-generated method stub
                        System.out.println("============================");
                        System.out.println(t);
                    }

                });
    }

    /**
     * repaitition其实只是coalesce的shuffle为true的简易的实现版本
     */

    public static void coalesce() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("distinct");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        /**N  代表的是原来的分区数
         * M numPartitions  新的分区数
         * shuffle  是否进行shuffle
         */
        listRDD.coalesce(2, true);
        /**
         * 1）N < M  需要将shuffle设置为true。
         2）N > M 相差不多，N=1000 M=100  建议 shuffle=false 。
         父RDD和子RDD是窄依赖
         3）N >> M  比如 n=100 m=1  建议shuffle设置为true，这样性能更好。
         设置为false，父RDD和子RDD是窄依赖，他们同在一个stage中。造成任务并行度不够，从而速度缓慢。
         */
    }


    //repartition  coalesce  重新进行分区  窄依赖，宽依赖  shuffle

    /**
     * filter 过滤了以后 --partition数据量会减少
     * 100 parition    task
     * 100  ->  50 parition  task
     * <p>
     * 这一个repartition分区，会进行shuffle操作。
     */
    public static void repartition() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("distinct");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        listRDD.repartition(2)
                .foreach(new VoidFunction<Integer>() {
                    public void call(Integer t) throws Exception {
                        System.out.println(t);
                    }
                });
    }


    /**
     * Return a new RDD by applying a function to each partition of this RDD.
     */
    public static void mapPartitions() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("distinct");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> listRDD = sc.parallelize(list, 4);// partition 1 :123  partition 2 : 4 5 6
        listRDD.mapPartitions(new FlatMapFunction<Iterator<Integer>, String>() {
            /**
             * 每次处理的就是一个分区的数
             */
            public Iterator<String> call(Iterator<Integer> t)
                    throws Exception {
                ArrayList<String> list = new ArrayList<String>();
                while (t.hasNext()) {
                    Integer i = t.next();
                    list.add("hello " + i);
                }
                return list.iterator();
            }
        }).foreach(new VoidFunction<String>() {

            public void call(String t) throws Exception {
                System.out.println(t);
            }
        });

    }

    /**
     * 求两个RDD的笛卡尔积
     * 假设集合A={a, b}，集合B={0, 1, 2}，则两个集合的笛卡尔积为{(a, 0), (a, 1),
     * (a, 2), (b, 0), (b, 1), (b, 2)}。
     */
    public static void cartesian() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("distinct");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> lista = Arrays.asList(1, 2, 3);
        List<String> listb = Arrays.asList("a", "b", "c");
        JavaRDD<Integer> listaRDD = sc.parallelize(lista);

        JavaRDD<String> listbRDD = sc.parallelize(listb);
        JavaPairRDD<Integer, String> cartesian = listaRDD.cartesian(listbRDD);
        cartesian.foreach(new VoidFunction<Tuple2<Integer, String>>() {

            public void call(Tuple2<Integer, String> t) throws Exception {
                // TODO Auto-generated method stub
                System.out.println(t._1 + "  " + t._2);
            }

        });
    }

    /**
     * 去重
     */
    public static void distinct() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("distinct");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> lista = Arrays.asList(1, 2, 3, 4, 4, 5, 5, 6);
        JavaRDD<Integer> listaRDD = sc.parallelize(lista);
        listaRDD.distinct()
                .foreach(new VoidFunction<Integer>() {

                    public void call(Integer t) throws Exception {
                        // TODO Auto-generated method stub
                        System.out.println(t);
                    }

                });
    }

    /**
     * 求两个rdd的交集
     */
    public static void Intersection() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("Intersection");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> lista = Arrays.asList(1, 2, 3, 4);
        List<Integer> listb = Arrays.asList(4, 5, 6, 7);
        JavaRDD<Integer> listaRDD = sc.parallelize(lista);
        JavaRDD<Integer> listbRDD = sc.parallelize(listb);
        listaRDD.intersection(listbRDD)
                .foreach(new VoidFunction<Integer>() {

                    public void call(Integer t) throws Exception {
                        // TODO Auto-generated method stub
                        System.out.println(t);
                    }

                });
    }

    /**
     * 求rdd并集，但是不去重
     */
    public static void union() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("union");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> lista = Arrays.asList(1, 2, 3, 4);
        List<Integer> listb = Arrays.asList(4, 5, 6, 7);
        JavaRDD<Integer> listaRDD = sc.parallelize(lista);
        JavaRDD<Integer> listbRDD = sc.parallelize(listb);
        JavaRDD<Integer> union = listaRDD.union(listbRDD);
        union.foreach(new VoidFunction<Integer>() {

            public void call(Integer t) throws Exception {
                // TODO Auto-generated method stub
                System.out.println(t);
            }

        });
    }


    /**
     * 这个实现根据两个要进行合并的两个RDD操作,生成一个CoGroupedRDD的实例,
     * 这个RDD的返回结果是把相同的key中两个RDD分别进行合并操作,最后返回的RDD的value是一个Pair的实例,
     * 这个实例包含两个Iterable的值,第一个值表示的是RDD1中相同KEY的值,第二个值表示的是RDD2中相同key的值.
     */

    public static void cogroup() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("cogroup");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, String>> listname = Arrays.asList(
                new Tuple2<Integer, String>(1, "东方不败"),
                new Tuple2<Integer, String>(2, "岳不群"),
                new Tuple2<Integer, String>(3, "令狐冲")
        );

        List<Tuple2<Integer, Integer>> listscores = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 99),
                new Tuple2<Integer, Integer>(2, 80),
                new Tuple2<Integer, Integer>(3, 85),
                new Tuple2<Integer, Integer>(1, 98),
                new Tuple2<Integer, Integer>(2, 79),
                new Tuple2<Integer, Integer>(3, 84)
        );
        JavaPairRDD<Integer, String> listnameRDD = sc.parallelizePairs(listname);
        JavaPairRDD<Integer, Integer> listscoresRDD = sc.parallelizePairs(listscores);
        //<1,tuple2<"东方不败" , {99,98}>>
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = listnameRDD.cogroup(listscoresRDD);
        cogroup.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {

            public void call(
                    Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t)
                    throws Exception {
                System.out.println("编号：" + t._1);
                //	Iterator<String> names = t._2._1.iterator();
                System.out.println("名字集合：" + t._2._1);
                //	Iterator<Integer> scores = t._2._2.iterator();
                //while
                System.out.println("分数单" + t._2._2);

            }

        });

    }


    /**
     * 相同的 key 做链接
     * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
     * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
     * (k, v2) is in `other`. Performs a hash join across the cluster.
     * def join[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (V, W)] = fromRDD(rdd.join(other))
     */
    public static void join() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("sortBykey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, String>> listname = Arrays.asList(
                new Tuple2<Integer, String>(1, "东方不败"),
                new Tuple2<Integer, String>(2, "岳不群"),
                new Tuple2<Integer, String>(3, "令狐冲")
        );

        List<Tuple2<Integer, Integer>> listscores = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 99),
                new Tuple2<Integer, Integer>(2, 80),
                new Tuple2<Integer, Integer>(3, 85)
        );
        JavaPairRDD<Integer, String> listnameRDD = sc.parallelizePairs(listname);
        JavaPairRDD<Integer, Integer> listscoresRDD = sc.parallelizePairs(listscores);
        JavaPairRDD<Integer, Tuple2<String, Integer>> join = listnameRDD.join(listscoresRDD);
        join.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {

            public void call(Tuple2<Integer, Tuple2<String, Integer>> t)
                    throws Exception {
                System.out.println("编号：" + t._1);
                System.out.println("姓名：" + t._2._1);
                System.out.println("分数：" + t._2._2);
                System.out.println("==================");
            }
        });
        /**
         * exec res
         * ---------
         编号：1
         姓名：东方不败
         分数：99
         ==================
         编号：3
         姓名：令狐冲
         分数：85
         ==================
         编号：2
         姓名：岳不群
         分数：80
         ==================
         */
    }


    public static void sortBykey() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("sortBykey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, String>> list = Arrays.asList(
                new Tuple2<Integer, String>(99, "东方不败"),
                new Tuple2<Integer, String>(80, "岳不群"),
                new Tuple2<Integer, String>(85, "令狐冲"),
                new Tuple2<Integer, String>(88, "仍我行")
        );
        JavaPairRDD<Integer, String> listRDD = sc.parallelizePairs(list);
        listRDD.sortByKey(false)
                .foreach(new VoidFunction<Tuple2<Integer, String>>() {

                    public void call(Tuple2<Integer, String> t) throws Exception {
                        // TODO Auto-generated method stub
                        System.out.println(t._2 + " : " + t._1);

                    }

                });
    }


    /**
     * the list can be directly  parallelizePairs to a JavaPairRdd
     * also can parallelize().mapToPair
     */
    public static void reduceBykey() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("reduceBykey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // create a tuple list
        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<String, Integer>("峨眉", 30),
                new Tuple2<String, Integer>("武当", 40),
                new Tuple2<String, Integer>("峨眉", 60),
                new Tuple2<String, Integer>("武当", 70)
        );

        JavaPairRDD<String, Integer> listRDD = sc.parallelizePairs(list);
//        JavaRDD<Tuple2<String, Integer>> listRDD = sc.parallelize(list);

        // map to a tuple, return JavaPairRDD, only exist in Java
        JavaPairRDD<String, Integer> mapToPair = listRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {

            public Tuple2<String, Integer> call(Tuple2<String, Integer> t)
                    throws Exception {
                // TODO Auto-generated method stub
                System.out.println("--mapToPair--> " + new Tuple2<String, Integer>(t._1, t._2));
                /**
                 *   --mapToPair--> (峨眉,30)
                 --mapToPair--> (武当,40)
                 --mapToPair--> (峨眉,60)
                 --mapToPair--> (武当,70)
                 */
                return new Tuple2<String, Integer>(t._1, t._2);
            }
        });
        /**
         * Integer, Integer,
         *  Integer  第三个integer指的是返回值类型
         * scala.reduceBykey(_+_)
         */
        listRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer v1, Integer v2) throws Exception {

                return v1 + v2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {

            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + " : " + t._2);
            }
        });
    }

    /**
     * 根据key进行分组，也就是意味着咱们的数据应该是key value的形式
     * Tuple2 对象类似于Java里面的map对象
     */
    public static void groupBykey() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("map");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, String>> list = Arrays.asList(
                new Tuple2<String, String>("峨眉", "周芷若"),
                new Tuple2<String, String>("武当", "宋青书"),
                new Tuple2<String, String>("峨眉", "灭绝师太"),
                new Tuple2<String, String>("武当", "张无忌")
        );
        //这个地方需要注意下，因为我们调用的是ByKey之类的操作，那么需要返回pairs的形式的元素
        JavaPairRDD<String, String> listRDD = sc.parallelizePairs(list);
        //JavaRDD<Tuple2<String, String>> parallelize = sc.parallelize(list);
        //parallelize.mapToPair(f)
        JavaPairRDD<String, Iterable<String>> groupByKey = listRDD.groupByKey();
        groupByKey.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {

            public void call(Tuple2<String, Iterable<String>> t)
                    throws Exception {
                System.out.println(t._1);
                Iterator<String> iterator = t._2.iterator();
                while (iterator.hasNext()) {
                    System.out.println(iterator.next());
                }
                System.out.println("===========================");

            }

        });
    }

    /**
     * 模拟一个集合，{“you jump”,"i	jump"}
     * 把每个单词单独拆开，然后每个单词单独成为一行
     */
    public static void flatMap() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //如果是在本地运行那么设置setmaster参数为local
        //如果不设置，默认就在集群模式下运行。
        conf.setMaster("local");
        //给任务设置一下名称。
        conf.setAppName("map");
        // ctrl + alt + o
        //创建好了程序的入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        //模拟一个集合，使用并行化的方式创建出来一个RDD。
        List<String> list = Arrays.asList("you	jump", "i	jump");
        JavaRDD<String> listRDD = sc.parallelize(list);
        //U代表的是FlatMapFunction 这个函数的返回值
        JavaRDD<String> flatMap = listRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split("\t")).iterator();
            }
        });

        flatMap.foreach(new VoidFunction<String>() {

            public void call(String t) throws Exception {
                System.out.println(t);

            }

        });
    }

    /**
     * 模拟一个集合，过滤出这个集合里面所有的偶数
     */
    public static void filter() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //如果是在本地运行那么设置setmaster参数为local
        //如果不设置，默认就在集群模式下运行。
        conf.setMaster("local");
        //给任务设置一下名称。
        conf.setAppName("filter");
        // ctrl + alt + o
        //创建好了程序的入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        //模拟一个集合，使用并行化的方式创建出来一个RDD。
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        JavaRDD<Integer> filter = listRDD.filter(new Function<Integer, Boolean>() {
            //返回值一个boolean值。
            public Boolean call(Integer i) throws Exception {

                return i % 2 == 0;
            }

        });
        filter.foreach(new VoidFunction<Integer>() {

            public void call(Integer t) throws Exception {
                System.out.println(t);

            }
        });
    }


    public static void map() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //如果是在本地运行那么设置setmaster参数为local
        //如果不设置，默认就在集群模式下运行。
        conf.setMaster("local");
        //给任务设置一下名称。
        conf.setAppName("map");
        //创建好了程序的入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        //模拟一个集合，使用并行化的方式创建出来一个RDD。
        List<String> list = Arrays.asList("张无忌", "赵敏", "周芷若");
        JavaRDD<String> listRDD = sc.parallelize(list);
        //R代表的是这个函数的返回值
        JavaRDD<String> map = listRDD.map(new Function<String, String>() {

            public String call(String str) throws Exception {

                return "hello " + str;
            }

        });

        map.foreach(new VoidFunction<String>() {

            public void call(String t) throws Exception {
                System.out.println(t);

            }
        });
    }


    public static void main(String[] args) {
//        	map();
//        	filter();
//        	flatMap();
//        	 groupBykey();
//        	 reduceBykey();
        //	sortBykey();
//        	join();
//        cogroup();
        //	 union();
//        	Intersection();
//        	 distinct();
//        	cartesian();
//        	mapPartitions();
        //	repartition();
        //	sample();
//        aggregateByKey();
        	mapPartitionsWithIndex();
//        repartitionAndSortWithinPartitions();
    }

}
