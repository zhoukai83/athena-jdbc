# athena-jdbc
English / [简体中文](#Chinese)  

This is Athena JDBC driver

Fork from https://github.com/burtcorp/athena-jdbc Afterwards, I add some features from that repository

We can access Amazon AWS Athena data through the Athena JDBC driver.

We can also use Athena JDBC driver in some SQL workbenches, such as (DataGrip, Dbeaver, SqlWorkBench) to access


Currently, there are several Athena JDBC drivers available:

1. Amazon's official AWS Athena JDBC driver: https://github.com/burtcorp/athena-jdbc
2. DataGrip and Dbeaver come with their own Athena JDBC driver
3. Open source on Github: https://github.com/burtcorp/athena-jdbc


I tried using both the built-in Datagrid and Amazon's official Athena JDBC driver, but unable to successfully connect to SqlWorkBench under certain special network environment conditions.

Later, I discovered the Athena jdbc on github: https://github.com/burtcorp/athena-jdbc

After testing, I found that SQL query can be executed, but it cannot be used when placed in Datagrid and SqlWorkBench

So we studied debugging JDBC drivers in Datagrip and SqlWorkBench, and found that the most important issue was the lack of several key functions in the DatabaseMetaData class


I able to use it after add these key functions. In the early stage, I mainly debugged in SqlWorkBench, but later found that DataGrip was more useful, so I later gave up support for SqlWorkBench

If you want to support it, a slight change should also be possible
<br>
<br>
<br>
<span id='Chinese'>中文介绍</a>
<br>
这是Athena JDBC driver

fork自https://github.com/burtcorp/athena-jdbc后，添加了一些功能

我们可以通过Athena JDBC driver来访问亚马逊AWS Athena数据。

也可以在一些SQL workbench里例如(DataGrip, Dbeaver, SqlWorkBench)使用Athena JDBC driver来访问

当前，有以下几种Athena JDBC driver:
1. 亚马逊官方的AWS Athena JDBC driver: https://github.com/burtcorp/athena-jdbc
2. DataGrip, Dbeaver有自带的Athena JDBC驱动
3. github上开源的： https://github.com/burtcorp/athena-jdbc

我尝试了下，使用Datagrip自带的和亚马逊官方的Athena JDBC驱动，都没能某种特殊的网络环境条件下，在Datagrip和SqlWorkBench连接成功。

后来发现github上有athena-jdbc的code: https://github.com/burtcorp/athena-jdbc

测试了下发现可以执行sql query,但是放到Datagrip和SqlWorkBench却用不了

于是研究了在Datagrip和SqlWorkBench里调试JDBC驱动，发现最主要的是DatabaseMetaData类里少了最主要的几个关键函数

把这几个关键函数加上就能用了。前期主要在SqlWorkBench里调试，后来发现DataGrip更好用，所以后来放弃了SqlWorkBench的支持

要是想支持的话，稍微改改应该也行.  

<br>
<br>  
  
This is a JDBC driver for AWS Athena.

update: 20221123 version: 0.5.1
1. support using AWS profile. 

Usage: add property in advance tab: profile, and then no need Authentication
2. optimize catalog
3. query timeout set to 120 seconds

update: 20221122:
support used in DataGrip

