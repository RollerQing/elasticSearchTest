package com.qf.test.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by Shi shuai RollerQing on 2019/11/20 17:04
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Product {
    //bigdata中框架名
    private String name;
    //作者
    private String author;
    //版本号
    private String version;
}
