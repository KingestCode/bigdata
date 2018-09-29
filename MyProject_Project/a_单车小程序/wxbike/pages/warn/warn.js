// pages/wallet/index.js

Page({
  data: {
    // 故障车编号
    bikeNo: "",
    // 故障类型数组
    types: []
  },


  // 勾选故障类型，获取类型值存入checkboxValue
  checkboxChange: function (e) {
    var values = e.detail.value;
    console.log(values)
    this.setData({
      types: values
    })
  },

  // 提交到服务器
  formSubmit: function (e) {
    var bikeNo = e.detail.value.bikeNo;
    var types = this.data.types;
    console.log(bikeNo)
    console.log(types)
    //1.向业务系统发生请求，将车辆状态置位报修
    //2.向日志系统记录log
  },
})