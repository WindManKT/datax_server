package com.wind.task.run.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.wind.doamin.R;
import com.wind.entity.task.Task;
import com.wind.task.run.service.TaskService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import net.sf.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Date;

@RestController
@RequestMapping("task")
@CrossOrigin
public class TaskController {
    @Autowired
    TaskService taskService;

    @ApiOperation(value = "新增")
    @PutMapping("insert")
    public R insert(@RequestBody Task task){
        JSONObject json = task.getJson();
        json.put("desc",task.getDesc());
        task.setJson(json);
        task.setJob(task.getJson().toString());
        return taskService.save(task)?R.ok():R.error();
    }

    @ApiOperation(value = "查询所有")
    @GetMapping("selectAll")
    public R selectAll(){
        return R.ok().data("list",taskService.list(null));
    }

    @ApiOperation(value = "分页查询")
    @GetMapping("{page}/{limit}")
    public R pageList(
            @ApiParam(name = "page", value = "当前页码", required = true)
            @PathVariable Long page,
            @ApiParam(name = "limit", value = "每页记录数", required = true)
            @PathVariable Long limit) {

        Page<Task> pageParam = new Page<>(page, limit);
        taskService.page(pageParam, null);
        return R.ok().data("page", pageParam);
    }

    @ApiOperation(value = "修改")
    @PostMapping("update")
    public R update(@RequestBody Task task){
        Task byId = taskService.getById(task.getId());
        task.setVersion(byId.getVersion());

        JSONObject json = task.getJson();
        json.put("desc",task.getDesc());
        task.setJson(json);
        return  taskService.updateById(task)?R.ok():R.error();
    }
    @ApiOperation(value = "发布")
    @PostMapping("release")
    public R release(@RequestBody Task task){
        Task byId = taskService.getById(task.getId());
        byId.setRele(task.getRele());
        byId.setReleTime(new Date());
        return  taskService.updateById(task)?R.ok():R.error();
    }

    @ApiOperation(value = "id单个删除")
    @DeleteMapping("{id}")
    public R delete(@PathVariable Long id){
        return taskService.removeById(id)?R.ok():R.error();
    }

    @ApiOperation(value = "id多个删除")
    @DeleteMapping("deleteById/{ids}")
    public R deleteIds(@PathVariable ArrayList ids){
       return taskService.removeByIds(ids)?R.ok():R.error();
    }


    @PostMapping("getTask")
    public Task getTask(@RequestBody Long id){
        return taskService.getById(id);
    }



}
