package controller;

import bean.CallLog;
import bean.QueryInfo;
import com.google.gson.Gson;
import dao.CallLogDAO;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;
import java.util.List;

@Controller
public class CallLogHandler {

    @RequestMapping("/queryCallLog")
    public String queryCallLog(Model model, QueryInfo queryInfo){
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("applicationContext.xml");

        CallLogDAO callLogDAO = applicationContext.getBean(CallLogDAO.class);

        HashMap<String, String> paramsMap = new HashMap<>();
        paramsMap.put("telephone", queryInfo.getTelephone());
        paramsMap.put("year", queryInfo.getYear());
        paramsMap.put("month", queryInfo.getMonth());
        paramsMap.put("day", queryInfo.getDay());

        List<CallLog> callLogList = callLogDAO.getCallLogList(paramsMap);
//        System.out.println(callLogList);

        //1月，2月，3月。。。。。。
        //1, 2, 32, 1, 5
        StringBuilder dateString = new StringBuilder();
        StringBuilder countString = new StringBuilder();
        StringBuilder durationString = new StringBuilder();

        for (CallLog callLog : callLogList) {
            if (Integer.valueOf(callLog.getMonth()) > 0) {
                dateString.append(callLog.getMonth()).append("月").append(",");
                countString.append(callLog.getCall_sum()).append(",");
                durationString.append(Float.valueOf(callLog.getCall_duration_sum()) / 60f).append(",");
            }
        }
        model.addAttribute("telephone", callLogList.get(0).getTelephone());
        model.addAttribute("name", callLogList.get(0).getName());
        model.addAttribute("date", dateString.deleteCharAt(dateString.length() - 1));
        model.addAttribute("count", countString.deleteCharAt(countString.length() - 1));
        model.addAttribute("duration", durationString.deleteCharAt(durationString.length() - 1));

        return "jsp/CallLogListEchart";
    }

    @ResponseBody
    @RequestMapping("/queryCallLog2")
    public String queryCallLog2(Model model, QueryInfo queryInfo){
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("applicationContext.xml");

        CallLogDAO callLogDAO = applicationContext.getBean(CallLogDAO.class);

        HashMap<String, String> paramsMap = new HashMap<>();
        paramsMap.put("telephone", queryInfo.getTelephone());
        paramsMap.put("year", queryInfo.getYear());
        paramsMap.put("month", queryInfo.getMonth());
        paramsMap.put("day", queryInfo.getDay());

        List<CallLog> callLogList = callLogDAO.getCallLogList(paramsMap);

        Gson gson = new Gson();
        String resultJson = gson.toJson(callLogList);
        return resultJson;
    }
}
