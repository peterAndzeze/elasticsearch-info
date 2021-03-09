package net.sw.search.controller;

import net.sw.search.service.PositionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.List;
import java.util.Map;
@Controller
public class PositionController {
    @Autowired
    private PositionService positionService;

    //测试范文页面
    @GetMapping({"/", "index"})
    public String indexPage() {
        return "index";
    }

    @GetMapping("/search/{keyword}/{pageNo}/{pageSize}")
    @ResponseBody
    public List<Map<String, Object>> searchPosition(@PathVariable("keyword") String keyword, @PathVariable("pageNo") int pageNo, @PathVariable("pageSize") int pageSize) throws IOException {
        List<Map<String, Object>> list = positionService.searchs(keyword, pageNo, pageSize);
        System.out.println(list);
        return list;
    }
    @RequestMapping("/importAll")
    @ResponseBody
    public String importAll(){
        positionService.importAll();
        return "success";
    }
}
