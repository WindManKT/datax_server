package com.wind.base.handler;


import com.wind.base.exception.NoloseException;
import com.wind.doamin.R;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

@Slf4j
@ControllerAdvice
public class NoloseExceptionHandler {

    @ExceptionHandler(Exception.class)
    @ResponseBody
    public R error(Exception e){
        log.error(e.getMessage());
        e.printStackTrace();
        return R.error().message("系统维护中").code(500);
    }

   /* @ExceptionHandler(ArithmeticException.class)
    @ResponseBody
    public R error(ArithmeticException e){
        e.printStackTrace();
        return R.error().message("执行了自定义异常");
    }*/

    @ExceptionHandler(NoloseException.class)
    @ResponseBody
    public R error(NoloseException e){
        log.error(e.getMessage());
        e.printStackTrace();
        return R.error().message(e.getMsg()).code(e.getCode());
    }
}
