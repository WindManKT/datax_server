package com.wind.datax.run.util;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//


import com.alibaba.datax.common.element.ColumnCast;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.AbstractContainer;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.ConfigurationValidate;
import com.alibaba.datax.core.util.ExceptionTracker;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.LoadUtil;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Engine {
    private static final Logger LOG = LoggerFactory.getLogger(com.alibaba.datax.core.Engine.class);
    private static String RUNTIME_MODE;

    public static String JOB_JSON;

    public static String DATAX_ID;
    public static String DESC;

    public Engine() {
    }

    public static void start(Configuration allConf) {
        ColumnCast.bind(allConf);
        LoadUtil.bind(allConf);
        boolean isJob = !"taskGroup".equalsIgnoreCase(allConf.getString("core.container.model"));
        int channelNumber = 0;
        int taskGroupId = -1;
        Object container;
        long instanceId;
        if (isJob) {
            allConf.set("core.container.job.mode", RUNTIME_MODE);
            container = new JobContainer(allConf);
            instanceId = allConf.getLong("core.container.job.id", 0L);
        } else {
            container = new TaskGroupContainer(allConf);
            instanceId = allConf.getLong("core.container.job.id");
            taskGroupId = allConf.getInt("core.container.taskGroup.id");
            channelNumber = allConf.getInt("core.container.taskGroup.channel");
        }

        boolean traceEnable = allConf.getBool("core.container.trace.enable", true);
        boolean perfReportEnable = allConf.getBool("core.dataXServer.reportPerfLog", true);
        if (instanceId == -1L) {
            perfReportEnable = false;
        }

        int priority = 0;

        try {
            priority = Integer.parseInt(System.getenv("SKYNET_PRIORITY"));
        } catch (NumberFormatException var13) {
            LOG.warn("prioriy set to 0, because NumberFormatException, the value is: " + System.getProperty("PROIORY"));
        }

        Configuration jobInfoConfig = allConf.getConfiguration("job.jobInfo");
        PerfTrace perfTrace = PerfTrace.getInstance(isJob, instanceId, taskGroupId, priority, traceEnable);
        perfTrace.setJobInfo(jobInfoConfig, perfReportEnable, channelNumber);
        ((AbstractContainer)container).start();
    }

    public static String filterJobConfiguration(Configuration configuration) {
        Configuration jobConfWithSetting = configuration.getConfiguration("job").clone();
        Configuration jobContent = jobConfWithSetting.getConfiguration("content");
        filterSensitiveConfiguration(jobContent);
        jobConfWithSetting.set("content", jobContent);
        return jobConfWithSetting.beautify();
    }

    public static Configuration filterSensitiveConfiguration(Configuration configuration) {
        Set<String> keys = configuration.getKeys();
        Iterator var2 = keys.iterator();

        while(var2.hasNext()) {
            String key = (String)var2.next();
            boolean isSensitive = StringUtils.endsWithIgnoreCase(key, "password") || StringUtils.endsWithIgnoreCase(key, "accessKey");
            if (isSensitive && configuration.get(key) instanceof String) {
                configuration.set(key, configuration.getString(key).replaceAll(".", "*"));
            }
        }

        return configuration;
    }

    public static void entry(String[] args) throws Throwable {
        Options options = new Options();
        options.addOption("job", true, "Job config.");
        options.addOption("jobid", true, "Job unique id.");
        options.addOption("mode", true, "Job runtime mode.");
        BasicParser parser = new BasicParser();
        CommandLine cl = parser.parse(options, args);
        String jobPath = cl.getOptionValue("job");
        String jobIdString = cl.getOptionValue("jobid");
        RUNTIME_MODE = cl.getOptionValue("mode");
        Configuration configuration = ConfigParser.parse(jobPath);
        long jobId;
        if (!"-1".equalsIgnoreCase(jobIdString)) {
            jobId = Long.parseLong(jobIdString);
        } else {
            String dscJobUrlPatternString = "/instance/(\\d{1,})/config.xml";
            String dsJobUrlPatternString = "/inner/job/(\\d{1,})/config";
            String dsTaskGroupUrlPatternString = "/inner/job/(\\d{1,})/taskGroup/";
            List<String> patternStringList = Arrays.asList(dscJobUrlPatternString, dsJobUrlPatternString, dsTaskGroupUrlPatternString);
            jobId = parseJobIdFromUrl(patternStringList, jobPath);
        }

        boolean isStandAloneMode = "standalone".equalsIgnoreCase(RUNTIME_MODE);
        if (!isStandAloneMode && jobId == -1L) {
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "非 standalone 模式必须在 URL 中提供有效的 jobId.");
        } else {
            configuration.set("core.container.job.id", jobId);
            VMInfo vmInfo = VMInfo.getVmInfo();
            if (vmInfo != null) {
                LOG.info(vmInfo.toString());
            }

            //Tod
            JOB_JSON = filterJobConfiguration(configuration);
            LOG.info("\n" + filterJobConfiguration(configuration) + "\n");
            LOG.debug(configuration.toJSON());
            ConfigurationValidate.doValidate(configuration);
            com.alibaba.datax.core.Engine engine = new com.alibaba.datax.core.Engine();
            start(configuration);
        }
    }

    private static long parseJobIdFromUrl(List<String> patternStringList, String url) {
        long result = -1L;
        Iterator var4 = patternStringList.iterator();

        do {
            if (!var4.hasNext()) {
                return result;
            }

            String patternString = (String)var4.next();
            result = doParseJobIdFromUrl(patternString, url);
        } while(result == -1L);

        return result;
    }

    private static long doParseJobIdFromUrl(String patternString, String url) {
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(url);
        return matcher.find() ? Long.parseLong(matcher.group(1)) : -1L;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = 0;

        try {
            entry(args);
        } catch (Throwable var6) {
            exitCode = 1;
            LOG.error("\n\n经DataX智能分析,该任务最可能的错误原因是:\n" + ExceptionTracker.trace(var6));
            if (var6 instanceof DataXException) {
                DataXException tempException = (DataXException)var6;
                ErrorCode errorCode = tempException.getErrorCode();
                if (errorCode instanceof FrameworkErrorCode) {
                    FrameworkErrorCode tempErrorCode = (FrameworkErrorCode)errorCode;
                    exitCode = tempErrorCode.toExitValue();
                }
            }

            System.exit(exitCode);
        }

        System.exit(exitCode);
    }



}

