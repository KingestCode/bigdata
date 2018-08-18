//app.js
App({
  onLaunch: function () {
    // 登录,然后获取到用户的唯一身份ID，用于以后记log
    wx.login({
      success: res => {
        //根据你的微信小程序的密钥到后台获取ID
        // 发送 res.code 到后台换取 openId, sessionKey, unionId
        if (res.code) {
          var appid = "wx446c6a30888bf0fe";
          var secret = "630f58959c516f914b720fd15f146395";
          var code = res.code;
          //发起网络请求
          wx.request({
            url: 'https://api.weixin.qq.com/sns/jscode2session?appid=' + appid + '&secret=' + secret + '&js_code=' + code + '&grant_type=authorization_code',
            success: function (r) {
              //获取到每个用户的对立id
              console.log(r.data.openid)
              //把openid保存到本地
              wx.setStorageSync('openid', r.data.openid)
            }
          })
        } else {
          console.log('获取用户登录态失败！' + res.errMsg)
        }
      }
    })
  },
  globalData: {
    userInfo: null
  }
})