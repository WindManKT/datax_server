package com.wind.datax.run.util;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.AbstractJobPlugin;
import com.alibaba.datax.common.plugin.JobPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.StrUtil;
import com.alibaba.datax.core.AbstractContainer;
import com.alibaba.datax.core.container.util.HookInvoker;
import com.alibaba.datax.core.container.util.JobAssignUtil;
import com.alibaba.datax.core.job.scheduler.AbstractScheduler;
import com.alibaba.datax.core.job.scheduler.processinner.StandAloneScheduler;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.statistics.container.communicator.job.StandAloneJobContainerCommunicator;
import com.alibaba.datax.core.statistics.plugin.DefaultJobPluginCollector;
import com.alibaba.datax.core.util.ErrorRecordChecker;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.ClassLoaderSwapper;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.alibaba.datax.dataxservice.face.domain.enums.ExecuteMode;
import com.alibaba.fastjson.JSON;
import com.wind.datax.run.client.KafkaClient;
import com.wind.datax.run.domain.Log;
import com.wind.datax.run.service.LogService;
import com.wind.utils.spring.SpringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;


public class JobContainer  extends AbstractContainer {
    private static final Logger LOG = LoggerFactory.getLogger(com.alibaba.datax.core.job.JobContainer.class);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private ClassLoaderSwapper classLoaderSwapper = ClassLoaderSwapper.newCurrentThreadClassLoaderSwapper();
    private long jobId;
    private String readerPluginName;
    private String writerPluginName;
    private Reader.Job jobReader;
    private com.alibaba.datax.common.spi.Writer.Job jobWriter;
    private Configuration userConf;
    private long startTimeStamp;
    private long endTimeStamp;
    private long startTransferTimeStamp;
    private long endTransferTimeStamp;
    private int needChannelNumber;
    private int totalStage = 1;
    private ErrorRecordChecker errorLimit;

    private KafkaClient kafkaClient = SpringUtil.getBean(KafkaClient.class);
    private LogService logService = SpringUtil.getBean(LogService.class);


    public JobContainer(Configuration configuration) {
        super(configuration);
        this.errorLimit = new ErrorRecordChecker(configuration);
    }

    public void start() {
        LOG.info("DataX jobContainer starts job.");
        boolean hasException = false;
        boolean isDryRun = false;
        boolean var11 = false;

        try {
            var11 = true;
            this.startTimeStamp = System.currentTimeMillis();
            isDryRun = this.configuration.getBool("job.setting.dryRun", false);
            if (isDryRun) {
                LOG.info("jobContainer starts to do preCheck ...");
                this.preCheck();
                var11 = false;
            } else {
                this.userConf = this.configuration.clone();
                LOG.debug("jobContainer starts to do preHandle ...");
                this.preHandle();
                LOG.debug("jobContainer starts to do init ...");
                this.init();
                LOG.info("jobContainer starts to do prepare ...");
                this.prepare();
                LOG.info("jobContainer starts to do split ...");
                this.totalStage = this.split();
                LOG.info("jobContainer starts to do schedule ...");
                this.schedule();
                LOG.debug("jobContainer starts to do post ...");
                this.post();
                LOG.debug("jobContainer starts to do postHandle ...");
                this.postHandle();
                LOG.info("DataX jobId [{}] completed successfully.", this.jobId);
                this.invokeHooks();
                var11 = false;
            }
        } catch (Throwable var12) {
            LOG.error("Exception when job run", var12);
            hasException = true;
            if (var12 instanceof OutOfMemoryError) {
                this.destroy();
                System.gc();
            }

            if (super.getContainerCommunicator() == null) {
                AbstractContainerCommunicator tempContainerCollector = new StandAloneJobContainerCommunicator(this.configuration);
                super.setContainerCommunicator(tempContainerCollector);
            }

            Communication communication = super.getContainerCommunicator().collect();
            communication.setThrowable(var12);
            communication.setTimestamp(this.endTimeStamp);
            Communication tempComm = new Communication();
            tempComm.setTimestamp(this.startTransferTimeStamp);
            Communication reportCommunication = CommunicationTool.getReportCommunication(communication, tempComm, this.totalStage);
            super.getContainerCommunicator().report(reportCommunication);
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, var12);
        } finally {
            if (var11) {
                if (!isDryRun) {
                    this.destroy();
                    this.endTimeStamp = System.currentTimeMillis();
                    if (!hasException) {
                        VMInfo vmInfo = VMInfo.getVmInfo();
                        if (vmInfo != null) {
                            vmInfo.getDelta(false);
                            LOG.info(vmInfo.totalString());
                        }

                        LOG.info(PerfTrace.getInstance().summarizeNoException());
                        this.logStatistics();
                    }
                }

            }
        }

        if (!isDryRun) {
            this.destroy();
            this.endTimeStamp = System.currentTimeMillis();
            if (!hasException) {
                VMInfo vmInfo = VMInfo.getVmInfo();
                if (vmInfo != null) {
                    vmInfo.getDelta(false);
                    LOG.info(vmInfo.totalString());
                }

                LOG.info(PerfTrace.getInstance().summarizeNoException());
                this.logStatistics();
            }
        }

    }

    private void preCheck() {
        this.preCheckInit();
        this.adjustChannelNumber();
        if (this.needChannelNumber <= 0) {
            this.needChannelNumber = 1;
        }

        this.preCheckReader();
        this.preCheckWriter();
        LOG.info("PreCheck通过");
    }

    private void preCheckInit() {
        this.jobId = this.configuration.getLong("core.container.job.id", -1L);
        if (this.jobId < 0L) {
            LOG.info("Set jobId = 0");
            this.jobId = 0L;
            this.configuration.set("core.container.job.id", this.jobId);
        }

        Thread.currentThread().setName("job-" + this.jobId);
        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(this.getContainerCommunicator());
        this.jobReader = this.preCheckReaderInit(jobPluginCollector);
        this.jobWriter = this.preCheckWriterInit(jobPluginCollector);
    }

    private Reader.Job preCheckReaderInit(JobPluginCollector jobPluginCollector) {
        this.readerPluginName = this.configuration.getString("job.content[0].reader.name");
        this.classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName));
        Reader.Job jobReader = (Reader.Job)LoadUtil.loadJobPlugin(PluginType.READER, this.readerPluginName);
        this.configuration.set("job.content[0].reader.parameter.dryRun", true);
        jobReader.setPluginJobConf(this.configuration.getConfiguration("job.content[0].reader.parameter"));
        jobReader.setPeerPluginJobConf(this.configuration.getConfiguration("job.content[0].reader.parameter"));
        jobReader.setJobPluginCollector(jobPluginCollector);
        this.classLoaderSwapper.restoreCurrentThreadClassLoader();
        return jobReader;
    }

    private com.alibaba.datax.common.spi.Writer.Job preCheckWriterInit(JobPluginCollector jobPluginCollector) {
        this.writerPluginName = this.configuration.getString("job.content[0].writer.name");
        this.classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName));
        com.alibaba.datax.common.spi.Writer.Job jobWriter = (com.alibaba.datax.common.spi.Writer.Job)LoadUtil.loadJobPlugin(PluginType.WRITER, this.writerPluginName);
        this.configuration.set("job.content[0].writer.parameter.dryRun", true);
        jobWriter.setPluginJobConf(this.configuration.getConfiguration("job.content[0].writer.parameter"));
        jobWriter.setPeerPluginJobConf(this.configuration.getConfiguration("job.content[0].reader.parameter"));
        jobWriter.setPeerPluginName(this.readerPluginName);
        jobWriter.setJobPluginCollector(jobPluginCollector);
        this.classLoaderSwapper.restoreCurrentThreadClassLoader();
        return jobWriter;
    }

    private void preCheckReader() {
        this.classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName));
        LOG.info(String.format("DataX Reader.Job [%s] do preCheck work .", this.readerPluginName));
        this.jobReader.preCheck();
        this.classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void preCheckWriter() {
        this.classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName));
        LOG.info(String.format("DataX Writer.Job [%s] do preCheck work .", this.writerPluginName));
        this.jobWriter.preCheck();
        this.classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void init() {
        this.jobId = this.configuration.getLong("core.container.job.id", -1L);
        if (this.jobId < 0L) {
            LOG.info("Set jobId = 0");
            this.jobId = 0L;
            this.configuration.set("core.container.job.id", this.jobId);
        }

        Thread.currentThread().setName("job-" + this.jobId);
        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(this.getContainerCommunicator());
        this.jobReader = this.initJobReader(jobPluginCollector);
        this.jobWriter = this.initJobWriter(jobPluginCollector);
    }

    private void prepare() {
        this.prepareJobReader();
        this.prepareJobWriter();
    }

    private void preHandle() {
        String handlerPluginTypeStr = this.configuration.getString("job.preHandler.pluginType");
        if (StringUtils.isNotEmpty(handlerPluginTypeStr)) {
            PluginType handlerPluginType;
            try {
                handlerPluginType = PluginType.valueOf(handlerPluginTypeStr.toUpperCase());
            } catch (IllegalArgumentException var6) {
                throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, String.format("Job preHandler's pluginType(%s) set error, reason(%s)", handlerPluginTypeStr.toUpperCase(), var6.getMessage()));
            }

            String handlerPluginName = this.configuration.getString("job.preHandler.pluginName");
            this.classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(handlerPluginType, handlerPluginName));
            AbstractJobPlugin handler = LoadUtil.loadJobPlugin(handlerPluginType, handlerPluginName);
            JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(this.getContainerCommunicator());
            handler.setJobPluginCollector(jobPluginCollector);
            handler.preHandler(this.configuration);
            this.classLoaderSwapper.restoreCurrentThreadClassLoader();
            LOG.info("After PreHandler: \n" + Engine.filterJobConfiguration(this.configuration) + "\n");
        }
    }

    private void postHandle() {
        String handlerPluginTypeStr = this.configuration.getString("job.postHandler.pluginType");
        if (StringUtils.isNotEmpty(handlerPluginTypeStr)) {
            PluginType handlerPluginType;
            try {
                handlerPluginType = PluginType.valueOf(handlerPluginTypeStr.toUpperCase());
            } catch (IllegalArgumentException var6) {
                throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, String.format("Job postHandler's pluginType(%s) set error, reason(%s)", handlerPluginTypeStr.toUpperCase(), var6.getMessage()));
            }

            String handlerPluginName = this.configuration.getString("job.postHandler.pluginName");
            this.classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(handlerPluginType, handlerPluginName));
            AbstractJobPlugin handler = LoadUtil.loadJobPlugin(handlerPluginType, handlerPluginName);
            JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(this.getContainerCommunicator());
            handler.setJobPluginCollector(jobPluginCollector);
            handler.postHandler(this.configuration);
            this.classLoaderSwapper.restoreCurrentThreadClassLoader();
        }
    }

    private int split() {
        this.adjustChannelNumber();
        if (this.needChannelNumber <= 0) {
            this.needChannelNumber = 1;
        }

        List<Configuration> readerTaskConfigs = this.doReaderSplit(this.needChannelNumber);
        int taskNumber = readerTaskConfigs.size();
        List<Configuration> writerTaskConfigs = this.doWriterSplit(taskNumber);
        List<Configuration> transformerList = this.configuration.getListConfiguration("job.content[0].transformer");
        LOG.debug("transformer configuration: " + JSON.toJSONString(transformerList));
        List<Configuration> contentConfig = this.mergeReaderAndWriterTaskConfigs(readerTaskConfigs, writerTaskConfigs, transformerList);
        LOG.debug("contentConfig configuration: " + JSON.toJSONString(contentConfig));
        this.configuration.set("job.content", contentConfig);
        return contentConfig.size();
    }

    private void adjustChannelNumber() {
        int needChannelNumberByByte = 2147483647;
        int needChannelNumberByRecord = 2147483647;
        boolean isByteLimit = this.configuration.getInt("job.setting.speed.byte", 0) > 0;
        if (isByteLimit) {
            long globalLimitedByteSpeed = (long)this.configuration.getInt("job.setting.speed.byte", 10485760);
            Long channelLimitedByteSpeed = this.configuration.getLong("core.transport.channel.speed.byte");
            if (channelLimitedByteSpeed == null || channelLimitedByteSpeed <= 0L) {
                throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "在有总bps限速条件下，单个channel的bps值不能为空，也不能为非正数");
            }

            needChannelNumberByByte = (int)(globalLimitedByteSpeed / channelLimitedByteSpeed);
            needChannelNumberByByte = needChannelNumberByByte > 0 ? needChannelNumberByByte : 1;
            LOG.info("Job set Max-Byte-Speed to " + globalLimitedByteSpeed + " bytes.");
        }

        boolean isRecordLimit = this.configuration.getInt("job.setting.speed.record", 0) > 0;
        if (isRecordLimit) {
            long globalLimitedRecordSpeed = (long)this.configuration.getInt("job.setting.speed.record", 100000);
            Long channelLimitedRecordSpeed = this.configuration.getLong("core.transport.channel.speed.record");
            if (channelLimitedRecordSpeed == null || channelLimitedRecordSpeed <= 0L) {
                throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "在有总tps限速条件下，单个channel的tps值不能为空，也不能为非正数");
            }

            needChannelNumberByRecord = (int)(globalLimitedRecordSpeed / channelLimitedRecordSpeed);
            needChannelNumberByRecord = needChannelNumberByRecord > 0 ? needChannelNumberByRecord : 1;
            LOG.info("Job set Max-Record-Speed to " + globalLimitedRecordSpeed + " records.");
        }

        this.needChannelNumber = needChannelNumberByByte < needChannelNumberByRecord ? needChannelNumberByByte : needChannelNumberByRecord;
        if (this.needChannelNumber >= 2147483647) {
            boolean isChannelLimit = this.configuration.getInt("job.setting.speed.channel", 0) > 0;
            if (isChannelLimit) {
                this.needChannelNumber = this.configuration.getInt("job.setting.speed.channel");
                LOG.info("Job set Channel-Number to " + this.needChannelNumber + " channels.");
            } else {
                throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "Job运行速度必须设置");
            }
        }
    }

    private void schedule() {
        int channelsPerTaskGroup = this.configuration.getInt("core.container.taskGroup.channel", 5);
        int taskNumber = this.configuration.getList("job.content").size();
        this.needChannelNumber = Math.min(this.needChannelNumber, taskNumber);
        PerfTrace.getInstance().setChannelNumber(this.needChannelNumber);
        List<Configuration> taskGroupConfigs = JobAssignUtil.assignFairly(this.configuration, this.needChannelNumber, channelsPerTaskGroup);
        LOG.info("Scheduler starts [{}] taskGroups.", taskGroupConfigs.size());
        ExecuteMode executeMode = null;

        try {
            executeMode = ExecuteMode.STANDALONE;
            AbstractScheduler scheduler = this.initStandaloneScheduler(this.configuration);
            Iterator var6 = taskGroupConfigs.iterator();

            while(true) {
                if (!var6.hasNext()) {
                    if ((executeMode == ExecuteMode.LOCAL || executeMode == ExecuteMode.DISTRIBUTE) && this.jobId <= 0L) {
                        throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, "在[ local | distribute ]模式下必须设置jobId，并且其值 > 0 .");
                    }

                    LOG.info("Running by {} Mode.", executeMode);
                    this.startTransferTimeStamp = System.currentTimeMillis();
                    scheduler.schedule(taskGroupConfigs);
                    this.endTransferTimeStamp = System.currentTimeMillis();
                    break;
                }

                Configuration taskGroupConfig = (Configuration)var6.next();
                taskGroupConfig.set("core.container.job.mode", executeMode.getValue());
            }
        } catch (Exception var8) {
            LOG.error("运行scheduler 模式[{}]出错.", executeMode);
            this.endTransferTimeStamp = System.currentTimeMillis();
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, var8);
        }

        this.checkLimit();
    }

    private AbstractScheduler initStandaloneScheduler(Configuration configuration) {
        AbstractContainerCommunicator containerCommunicator = new StandAloneJobContainerCommunicator(configuration);
        super.setContainerCommunicator(containerCommunicator);
        return new StandAloneScheduler(containerCommunicator);
    }

    private void post() {
        this.postJobWriter();
        this.postJobReader();
    }

    private void destroy() {
        if (this.jobWriter != null) {
            this.jobWriter.destroy();
            this.jobWriter = null;
        }

        if (this.jobReader != null) {
            this.jobReader.destroy();
            this.jobReader = null;
        }

    }

    private void logStatistics() {
        long totalCosts = (this.endTimeStamp - this.startTimeStamp) / 1000L;
        long transferCosts = (this.endTransferTimeStamp - this.startTransferTimeStamp) / 1000L;
        if (0L == transferCosts) {
            transferCosts = 1L;
        }

        if (super.getContainerCommunicator() != null) {
            Communication communication = super.getContainerCommunicator().collect();
            communication.setTimestamp(this.endTimeStamp);
            Communication tempComm = new Communication();
            tempComm.setTimestamp(this.startTransferTimeStamp);
            Communication reportCommunication = CommunicationTool.getReportCommunication(communication, tempComm, this.totalStage);
            long byteSpeedPerSecond = communication.getLongCounter("readSucceedBytes") / transferCosts;
            long recordSpeedPerSecond = communication.getLongCounter("readSucceedRecords") / transferCosts;
            reportCommunication.setLongCounter("byteSpeed", byteSpeedPerSecond);
            reportCommunication.setLongCounter("recordSpeed", recordSpeedPerSecond);
            super.getContainerCommunicator().report(reportCommunication);
            LOG.info(String.format("\n%-26s: %-18s\n%-26s: %-18s\n%-26s: %19s\n%-26s: %19s\n%-26s: %19s\n%-26s: %19s\n%-26s: %19s\n", "任务启动时刻", dateFormat.format(this.startTimeStamp), "任务结束时刻", dateFormat.format(this.endTimeStamp), "任务总计耗时", totalCosts + "s", "任务平均流量", StrUtil.stringify(byteSpeedPerSecond) + "/s", "记录写入速度", recordSpeedPerSecond + "rec/s", "读出记录总数", String.valueOf(CommunicationTool.getTotalReadRecords(communication)), "读写失败总数", String.valueOf(CommunicationTool.getTotalErrorRecords(communication))));
            String.format("\n%-26s: %-18s\n%-26s: %-18s\n%-26s: %19s\n%-26s: %19s\n%-26s: %19s\n%-26s: %19s\n%-26s: %19s\n", "任务启动时刻", dateFormat.format(this.startTimeStamp), "任务结束时刻", dateFormat.format(this.endTimeStamp), "任务总计耗时", totalCosts + "s", "任务平均流量", StrUtil.stringify(byteSpeedPerSecond) + "/s", "记录写入速度", recordSpeedPerSecond + "rec/s", "读出记录总数", String.valueOf(CommunicationTool.getTotalReadRecords(communication)), "读写失败总数", String.valueOf(CommunicationTool.getTotalErrorRecords(communication)));

            HashMap map = new HashMap();
            String value =String.format(
                            "\n%-26s: %-18s\n%-26s: %-18s\n%-26s: %19s\n%-26s: %19s\n%-26s: %19s\n%-26s: %19s\n%-26s: %19s\n",
                            "任务启动时刻",dateFormat.format(this.startTimeStamp),
                            "任务结束时刻", dateFormat.format(this.endTimeStamp),
                            "任务总计耗时", totalCosts + "s",
                            "任务平均流量", StrUtil.stringify(byteSpeedPerSecond) + "/s",
                            "记录写入速度", recordSpeedPerSecond + "rec/s",
                            "读出记录总数",String.valueOf(CommunicationTool.getTotalReadRecords(communication)),
                            "读写失败总数", String.valueOf(CommunicationTool.getTotalErrorRecords(communication))) +
                     "\n"+"|描述信息===>" + Engine.DESC;
            System.err.println(value);
            Log log = new Log();
            log.setLog(value);
            log.setDataxId(Engine.DATAX_ID);
            logService.save(log);
            map.put("datax", Arrays.asList(value));
            kafkaClient.saveData(map);

            if (communication.getLongCounter("totalTransformerSuccessRecords") > 0L || communication.getLongCounter("totalTransformerFailedRecords") > 0L || communication.getLongCounter("totalTransformerFilterRecords") > 0L) {
                LOG.info(String.format("\n%-26s: %19s\n%-26s: %19s\n%-26s: %19s\n", "Transformer成功记录总数", communication.getLongCounter("totalTransformerSuccessRecords"), "Transformer失败记录总数", communication.getLongCounter("totalTransformerFailedRecords"), "Transformer过滤记录总数", communication.getLongCounter("totalTransformerFilterRecords")));
            }

        }
    }

    private Reader.Job initJobReader(JobPluginCollector jobPluginCollector) {
        this.readerPluginName = this.configuration.getString("job.content[0].reader.name");
        this.classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName));
        Reader.Job jobReader = (Reader.Job)LoadUtil.loadJobPlugin(PluginType.READER, this.readerPluginName);
        jobReader.setPluginJobConf(this.configuration.getConfiguration("job.content[0].reader.parameter"));
        jobReader.setPeerPluginJobConf(this.configuration.getConfiguration("job.content[0].writer.parameter"));
        jobReader.setJobPluginCollector(jobPluginCollector);
        jobReader.init();
        this.classLoaderSwapper.restoreCurrentThreadClassLoader();
        return jobReader;
    }

    private com.alibaba.datax.common.spi.Writer.Job initJobWriter(JobPluginCollector jobPluginCollector) {
        this.writerPluginName = this.configuration.getString("job.content[0].writer.name");
        this.classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName));
        com.alibaba.datax.common.spi.Writer.Job jobWriter = (com.alibaba.datax.common.spi.Writer.Job)LoadUtil.loadJobPlugin(PluginType.WRITER, this.writerPluginName);
        jobWriter.setPluginJobConf(this.configuration.getConfiguration("job.content[0].writer.parameter"));
        jobWriter.setPeerPluginJobConf(this.configuration.getConfiguration("job.content[0].reader.parameter"));
        jobWriter.setPeerPluginName(this.readerPluginName);
        jobWriter.setJobPluginCollector(jobPluginCollector);
        jobWriter.init();
        this.classLoaderSwapper.restoreCurrentThreadClassLoader();
        return jobWriter;
    }

    private void prepareJobReader() {
        this.classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName));
        LOG.info(String.format("DataX Reader.Job [%s] do prepare work .", this.readerPluginName));
        this.jobReader.prepare();
        this.classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void prepareJobWriter() {
        this.classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName));
        LOG.info(String.format("DataX Writer.Job [%s] do prepare work .", this.writerPluginName));
        this.jobWriter.prepare();
        this.classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private List<Configuration> doReaderSplit(int adviceNumber) {
        this.classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName));
        List<Configuration> readerSlicesConfigs = this.jobReader.split(adviceNumber);
        if (readerSlicesConfigs != null && readerSlicesConfigs.size() > 0) {
            LOG.info("DataX Reader.Job [{}] splits to [{}] tasks.", this.readerPluginName, readerSlicesConfigs.size());
            this.classLoaderSwapper.restoreCurrentThreadClassLoader();
            return readerSlicesConfigs;
        } else {
            throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_SPLIT_ERROR, "reader切分的task数目不能小于等于0");
        }
    }

    private List<Configuration> doWriterSplit(int readerTaskNumber) {
        this.classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName));
        List<Configuration> writerSlicesConfigs = this.jobWriter.split(readerTaskNumber);
        if (writerSlicesConfigs != null && writerSlicesConfigs.size() > 0) {
            LOG.info("DataX Writer.Job [{}] splits to [{}] tasks.", this.writerPluginName, writerSlicesConfigs.size());
            this.classLoaderSwapper.restoreCurrentThreadClassLoader();
            return writerSlicesConfigs;
        } else {
            throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_SPLIT_ERROR, "writer切分的task不能小于等于0");
        }
    }

    private List<Configuration> mergeReaderAndWriterTaskConfigs(List<Configuration> readerTasksConfigs, List<Configuration> writerTasksConfigs) {
        return this.mergeReaderAndWriterTaskConfigs(readerTasksConfigs, writerTasksConfigs, (List)null);
    }

    private List<Configuration> mergeReaderAndWriterTaskConfigs(List<Configuration> readerTasksConfigs, List<Configuration> writerTasksConfigs, List<Configuration> transformerConfigs) {
        if (readerTasksConfigs.size() != writerTasksConfigs.size()) {
            throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_SPLIT_ERROR, String.format("reader切分的task数目[%d]不等于writer切分的task数目[%d].", readerTasksConfigs.size(), writerTasksConfigs.size()));
        } else {
            List<Configuration> contentConfigs = new ArrayList();

            for(int i = 0; i < readerTasksConfigs.size(); ++i) {
                Configuration taskConfig = Configuration.newDefault();
                taskConfig.set("reader.name", this.readerPluginName);
                taskConfig.set("reader.parameter", readerTasksConfigs.get(i));
                taskConfig.set("writer.name", this.writerPluginName);
                taskConfig.set("writer.parameter", writerTasksConfigs.get(i));
                if (transformerConfigs != null && transformerConfigs.size() > 0) {
                    taskConfig.set("transformer", transformerConfigs);
                }

                taskConfig.set("taskId", i);
                contentConfigs.add(taskConfig);
            }

            return contentConfigs;
        }
    }

    private List<Configuration> distributeTasksToTaskGroup(int averTaskPerChannel, int channelNumber, int channelsPerTaskGroup) {
        Validate.isTrue(averTaskPerChannel > 0 && channelNumber > 0 && channelsPerTaskGroup > 0, "每个channel的平均task数[averTaskPerChannel]，channel数目[channelNumber]，每个taskGroup的平均channel数[channelsPerTaskGroup]都应该为正数");
        List<Configuration> taskConfigs = this.configuration.getListConfiguration("job.content");
        int taskGroupNumber = channelNumber / channelsPerTaskGroup;
        int leftChannelNumber = channelNumber % channelsPerTaskGroup;
        if (leftChannelNumber > 0) {
            ++taskGroupNumber;
        }

        if (taskGroupNumber == 1) {
            final Configuration taskGroupConfig = this.configuration.clone();
            taskGroupConfig.set("job.content", this.configuration.getListConfiguration("job.content"));
            taskGroupConfig.set("core.container.taskGroup.channel", channelNumber);
            taskGroupConfig.set("core.container.taskGroup.id", 0);
            return new ArrayList<Configuration>() {
                {
                    this.add(taskGroupConfig);
                }
            };
        } else {
            List<Configuration> taskGroupConfigs = new ArrayList();

            int taskConfigIndex;
            for(taskConfigIndex = 0; taskConfigIndex < taskGroupNumber; ++taskConfigIndex) {
                Configuration taskGroupConfig = this.configuration.clone();
                List<Configuration> taskGroupJobContent = taskGroupConfig.getListConfiguration("job.content");
                taskGroupJobContent.clear();
                taskGroupConfig.set("job.content", taskGroupJobContent);
                taskGroupConfigs.add(taskGroupConfig);
            }

            taskConfigIndex = 0;
            int channelIndex = 0;
            int taskGroupConfigIndex = 0;
            List taskGroupJobContent;
            if (leftChannelNumber > 0) {
                Configuration taskGroupConfig;
                for(taskGroupConfig = (Configuration)taskGroupConfigs.get(taskGroupConfigIndex); channelIndex < leftChannelNumber; ++channelIndex) {
                    for(int i = 0; i < averTaskPerChannel; ++i) {
                        taskGroupJobContent = taskGroupConfig.getListConfiguration("job.content");
                        taskGroupJobContent.add(taskConfigs.get(taskConfigIndex++));
                        taskGroupConfig.set("job.content", taskGroupJobContent);
                    }
                }

                taskGroupConfig.set("core.container.taskGroup.channel", leftChannelNumber);
                taskGroupConfig.set("core.container.taskGroup.id", taskGroupConfigIndex++);
            }

            int equalDivisionStartIndex = taskGroupConfigIndex;

            Configuration taskGroupConfig;
            while(taskConfigIndex < taskConfigs.size() && equalDivisionStartIndex < taskGroupConfigs.size()) {
                for(taskGroupConfigIndex = equalDivisionStartIndex; taskGroupConfigIndex < taskGroupConfigs.size() && taskConfigIndex < taskConfigs.size(); ++taskGroupConfigIndex) {
                    taskGroupConfig = (Configuration)taskGroupConfigs.get(taskGroupConfigIndex);
                    taskGroupJobContent = taskGroupConfig.getListConfiguration("job.content");
                    taskGroupJobContent.add(taskConfigs.get(taskConfigIndex++));
                    taskGroupConfig.set("job.content", taskGroupJobContent);
                }
            }

            taskGroupConfigIndex = equalDivisionStartIndex;

            while(taskGroupConfigIndex < taskGroupConfigs.size()) {
                taskGroupConfig = (Configuration)taskGroupConfigs.get(taskGroupConfigIndex);
                taskGroupConfig.set("core.container.taskGroup.channel", channelsPerTaskGroup);
                taskGroupConfig.set("core.container.taskGroup.id", taskGroupConfigIndex++);
            }

            return taskGroupConfigs;
        }
    }

    private void postJobReader() {
        this.classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, this.readerPluginName));
        LOG.info("DataX Reader.Job [{}] do post work.", this.readerPluginName);
        this.jobReader.post();
        this.classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void postJobWriter() {
        this.classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, this.writerPluginName));
        LOG.info("DataX Writer.Job [{}] do post work.", this.writerPluginName);
        this.jobWriter.post();
        this.classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void checkLimit() {
        Communication communication = super.getContainerCommunicator().collect();
        this.errorLimit.checkRecordLimit(communication);
        this.errorLimit.checkPercentageLimit(communication);
    }

    private void invokeHooks() {
        Communication comm = super.getContainerCommunicator().collect();
        HookInvoker invoker = new HookInvoker(CoreConstant.DATAX_HOME + "/hook", this.configuration, comm.getCounter());
        invoker.invokeAll();
    }
}
