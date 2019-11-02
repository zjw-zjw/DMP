package com.zjw.dmp.utils

import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.{GetMethod, PostMethod}

object HttpUtils {

  /**
    * 发起请求
    * @param url 请求路径
    * @return  json字符串的返回结果
    */
  def get(url:String): String = {
    // 1. 获取httpClient
    val httpClient: HttpClient = new HttpClient()
    // 2. 获取请求方式 : Get请求
    val get = new GetMethod(url)
    // 3. 发起请求
    val code: Int = httpClient.executeMethod(get)
    // 4. 判断状态码是否为200, 200才是正确发送请求
    if(code == 200) {
      // 返回结果
      get.getResponseBodyAsString
    } else {
      ""
    }

  }
}
