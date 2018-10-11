package dao;

import bean.CallLog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public class CallLogDAO {
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    @Autowired
    public CallLogDAO(NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
    }

    public List<CallLog> getCallLogList(Map<String, String> paramsMap) {
        String sql = "SELECT t3.*, t4.`year`, t4.`month`, t4.`day` FROM ( SELECT t1.id_date_contact, t1.id_date_dimension, t1.id_contact, t1.call_sum, t1.call_duration_sum , t2.telephone, t2.`name` FROM tb_call t1 JOIN tb_contacts t2 ON t1.id_contact = t2.id ) t3 JOIN tb_dimension_date t4 ON t3.id_date_dimension = t4.id WHERE telephone = :telephone AND `year` = :year AND `month` != :month AND `day` = :day ORDER BY `year`, `month`, `day`;";
        BeanPropertyRowMapper<CallLog> beanPropertyRowMapper = new BeanPropertyRowMapper<>(CallLog.class);
        return namedParameterJdbcTemplate.query(sql, paramsMap, beanPropertyRowMapper);
    }
}
