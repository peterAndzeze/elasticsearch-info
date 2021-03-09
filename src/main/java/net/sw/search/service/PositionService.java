package net.sw.search.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface PositionService {
    /**
     *分页查询
     * @param keyWord
     * @param pageNo
     * @param pageSize
     * @return
     */
    public List<Map<String,Object>> searchs(String keyWord,int pageNo,int pageSize) throws IOException;

    /**
     * 导入数据
     */
    public void importAll();
}
