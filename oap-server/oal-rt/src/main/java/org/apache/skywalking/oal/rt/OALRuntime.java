/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oal.rt;

import freemarker.template.Configuration;
import freemarker.template.Version;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtField;
import javassist.CtNewConstructor;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ClassFile;
import javassist.bytecode.ConstPool;
import javassist.bytecode.SignatureAttribute;
import javassist.bytecode.annotation.Annotation;
import javassist.bytecode.annotation.ClassMemberValue;
import javassist.bytecode.annotation.IntegerMemberValue;
import javassist.bytecode.annotation.StringMemberValue;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.SystemUtils;
import org.apache.skywalking.oal.rt.output.AllDispatcherContext;
import org.apache.skywalking.oal.rt.output.DispatcherContext;
import org.apache.skywalking.oal.rt.parser.AnalysisResult;
import org.apache.skywalking.oal.rt.parser.OALScripts;
import org.apache.skywalking.oal.rt.parser.ScriptParser;
import org.apache.skywalking.oal.rt.parser.SourceColumn;
import org.apache.skywalking.oap.server.core.WorkPath;
import org.apache.skywalking.oap.server.core.analysis.DisableRegister;
import org.apache.skywalking.oap.server.core.analysis.DispatcherDetectorListener;
import org.apache.skywalking.oap.server.core.analysis.SourceDispatcher;
import org.apache.skywalking.oap.server.core.analysis.Stream;
import org.apache.skywalking.oap.server.core.analysis.StreamAnnotationListener;
import org.apache.skywalking.oap.server.core.oal.rt.OALCompileException;
import org.apache.skywalking.oap.server.core.oal.rt.OALDefine;
import org.apache.skywalking.oap.server.core.oal.rt.OALEngine;
import org.apache.skywalking.oap.server.core.source.oal.rt.dispatcher.DispatcherClassPackageHolder;
import org.apache.skywalking.oap.server.core.source.oal.rt.metrics.MetricClassPackageHolder;
import org.apache.skywalking.oap.server.core.source.oal.rt.metrics.builder.MetricBuilderClassPackageHolder;
import org.apache.skywalking.oap.server.core.storage.StorageBuilderFactory;
import org.apache.skywalking.oap.server.core.storage.StorageException;
import org.apache.skywalking.oap.server.core.storage.annotation.BanyanDB;
import org.apache.skywalking.oap.server.core.storage.annotation.Column;
import org.apache.skywalking.oap.server.library.module.ModuleStartException;
import org.apache.skywalking.oap.server.library.util.ResourceUtils;
import org.apache.skywalking.oap.server.library.util.StringUtil;

/**
 * OAL Runtime is the class generation engine, which load the generated classes from OAL scrip definitions. This runtime
 * is loaded dynamically.
 */
@Slf4j
public class OALRuntime implements OALEngine {

    private static final String CLASS_FILE_CHARSET = "UTF-8";
    private static final String METRICS_FUNCTION_PACKAGE = "org.apache.skywalking.oap.server.core.analysis.metrics.";
    private static final String WITH_METADATA_INTERFACE = "org.apache.skywalking.oap.server.core.analysis.metrics.WithMetadata";
    private static final String DISPATCHER_INTERFACE = "org.apache.skywalking.oap.server.core.analysis.SourceDispatcher";
    private static final String METRICS_STREAM_PROCESSOR = "org.apache.skywalking.oap.server.core.analysis.worker.MetricsStreamProcessor";
    private static final String[] METRICS_CLASS_METHODS = {
        "id",
        "hashCode",
        "remoteHashCode",
        "equals",
        "serialize",
        "deserialize",
        "getMeta",
        "toHour",
        "toDay"
    };
    private static final String[] METRICS_BUILDER_CLASS_METHODS = {
        "entity2Storage",
        "storage2Entity"
    };
    private static boolean IS_RT_TEMP_FOLDER_INIT_COMPLETED = false;

    private final OALDefine oalDefine;
    private final ClassPool classPool;
    private ClassLoader currentClassLoader;
    private Configuration configuration;
    private AllDispatcherContext allDispatcherContext;
    private StreamAnnotationListener streamAnnotationListener;
    private DispatcherDetectorListener dispatcherDetectorListener;
    private StorageBuilderFactory storageBuilderFactory;
    private final List<Class> metricsClasses;
    private final List<Class> dispatcherClasses;
    private final boolean openEngineDebug;

    // 该构造方法在 OALEngineLoaderService#loadOALEngine 方法中通过反射的方式调用
    public OALRuntime(OALDefine define) {
        oalDefine = define;
        classPool = ClassPool.getDefault();
        // freemarker 配置
        configuration = new Configuration(new Version("2.3.28"));
        configuration.setEncoding(Locale.ENGLISH, CLASS_FILE_CHARSET);
        configuration.setClassLoaderForTemplateLoading(OALRuntime.class.getClassLoader(), "/code-templates");
        allDispatcherContext = new AllDispatcherContext();
        metricsClasses = new ArrayList<>();
        dispatcherClasses = new ArrayList<>();
        // 启动时配置 SW_OAL_ENGINE_DEBUG=true 可以开启 debug 模式
        openEngineDebug = StringUtil.isNotEmpty(System.getenv("SW_OAL_ENGINE_DEBUG"));
    }

    @Override
    public void setStreamListener(StreamAnnotationListener listener) throws ModuleStartException {
        this.streamAnnotationListener = listener;
    }

    @Override
    public void setDispatcherListener(DispatcherDetectorListener listener) throws ModuleStartException {
        dispatcherDetectorListener = listener;
    }

    @Override
    public void setStorageBuilderFactory(final StorageBuilderFactory factory) {
        storageBuilderFactory = factory;
    }

    @Override
    public void start(ClassLoader currentClassLoader) throws ModuleStartException, OALCompileException {
        if (!IS_RT_TEMP_FOLDER_INIT_COMPLETED) {
            prepareRTTempFolder();
            IS_RT_TEMP_FOLDER_INIT_COMPLETED = true;
        }

        this.currentClassLoader = currentClassLoader;
        Reader read;

        try {
            read = ResourceUtils.read(oalDefine.getConfigFile());
        } catch (FileNotFoundException e) {
            throw new ModuleStartException("Can't locate " + oalDefine.getConfigFile(), e);
        }

        OALScripts oalScripts;
        try {
            ScriptParser scriptParser = ScriptParser.createFromFile(read, oalDefine.getSourcePackage());
            // 解析 OAL 配置
            oalScripts = scriptParser.parse();
        } catch (IOException e) {
            throw new ModuleStartException("OAL script parse analysis failure.", e);
        }
        // 动态生成需要的类
        this.generateClassAtRuntime(oalScripts);
    }

    @Override
    public void notifyAllListeners() throws ModuleStartException {
        for (Class metricsClass : metricsClasses) {
            try {
                // @Stream 注解，创建 metrics 的  work flow
                streamAnnotationListener.notify(metricsClass);
            } catch (StorageException e) {
                throw new ModuleStartException(e.getMessage(), e);
            }
        }
        for (Class dispatcherClass : dispatcherClasses) {
            try {
                // dispatcher 放入 dispatcherMap，将 source 和 dispatcher 关联 <sourceScopeId, [dispatcher...]>
                dispatcherDetectorListener.addIfAsSourceDispatcher(dispatcherClass);
            } catch (Exception e) {
                throw new ModuleStartException(e.getMessage(), e);
            }
        }
    }

    private void generateClassAtRuntime(OALScripts oalScripts) throws OALCompileException {
        List<AnalysisResult> metricsStmts = oalScripts.getMetricsStmts();
        metricsStmts.forEach(this::buildDispatcherContext);

        for (AnalysisResult metricsStmt : metricsStmts) {
            // 生成 metrics 类
            metricsClasses.add(generateMetricsClass(metricsStmt));
            // 生成 metrics 类对应的 builder 类（用于 entity 和 storage 之间的转换）
            // InstanceJvmOldGcTimeMetrics -> InstanceJvmOldGcTimeMetricsBuilder
            generateMetricsBuilderClass(metricsStmt);
        }

        for (Map.Entry<String, DispatcherContext> entry : allDispatcherContext.getAllContext().entrySet()) {
            // 生成 Dispatcher 类，一个 source（有多个metrics）对应一个 dispatcher 类
            dispatcherClasses.add(generateDispatcherClass(entry.getKey(), entry.getValue()));
        }

        oalScripts.getDisableCollection().getAllDisableSources().forEach(disable -> {
            DisableRegister.INSTANCE.add(disable);
        });
    }

    /**
     * Generate metrics class, and inject it to classloader
     */
    private Class generateMetricsClass(AnalysisResult metricsStmt) throws OALCompileException {
        // instance_jvm_old_gc_time = from(ServiceInstanceJVMGC.time).filter(phase == GCPhase.OLD).sum();
        // instance_jvm_old_gc_time -> InstanceJvmOldGcTime + Metrics -> InstanceJvmOldGcTimeMetrics（要生成的类名）
        String className = metricsClassName(metricsStmt, false);
        CtClass parentMetricsClass = null;
        try {
            // aggregationFunctionName 聚合函数对应的类
            // sum -> SumMetrics 作为待生成类的父类 -> InstanceJvmOldGcTimeMetrics extends SumMetrics
            parentMetricsClass = classPool.get(METRICS_FUNCTION_PACKAGE + metricsStmt.getMetricsClassName());
        } catch (NotFoundException e) {
            log.error("Can't find parent class for " + className + ".", e);
            throw new OALCompileException(e.getMessage(), e);
        }
        // 生成 Metrics 类
        CtClass metricsClass = classPool.makeClass(metricsClassName(metricsStmt, true), parentMetricsClass);
        try {
            // 实现 WithMetadata 接口
            metricsClass.addInterface(classPool.get(WITH_METADATA_INTERFACE));
        } catch (NotFoundException e) {
            log.error("Can't find WithMetadata interface for " + className + ".", e);
            throw new OALCompileException(e.getMessage(), e);
        }

        ClassFile metricsClassClassFile = metricsClass.getClassFile();
        // 常量池
        ConstPool constPool = metricsClassClassFile.getConstPool();

        /**
         * Create empty construct
         * 空构造函数
         */
        try {
            CtConstructor defaultConstructor = CtNewConstructor.make("public " + className + "() {}", metricsClass);
            metricsClass.addConstructor(defaultConstructor);
        } catch (CannotCompileException e) {
            log.error("Can't add empty constructor in " + className + ".", e);
            throw new OALCompileException(e.getMessage(), e);
        }

        /**
         * Add fields with annotations.
         *
         * private ${sourceField.typeName} ${sourceField.fieldName};
         */
        for (SourceColumn field : metricsStmt.getFieldsFromSource()) {
            try {
                CtField newField = CtField.make(
                    "private " + field.getType()
                                      .getName() + " " + field.getFieldName() + ";", metricsClass);
                // 添加属性
                metricsClass.addField(newField);
                // 为属性添加 getter setter 方法
                metricsClass.addMethod(CtNewMethod.getter(field.getFieldGetter(), newField));
                metricsClass.addMethod(CtNewMethod.setter(field.getFieldSetter(), newField));

                AnnotationsAttribute annotationsAttribute = new AnnotationsAttribute(
                    constPool, AnnotationsAttribute.visibleTag);
                /**
                 * Add @Column(columnName = "${sourceField.columnName}")
                 */
                Annotation columnAnnotation = new Annotation(Column.class.getName(), constPool);
                columnAnnotation.addMemberValue("columnName", new StringMemberValue(field.getColumnName(), constPool));
                if (field.getType().equals(String.class)) {
                    columnAnnotation.addMemberValue("length", new IntegerMemberValue(constPool, field.getLength()));
                }
                if (field.isID()) {
                    // Add shardingKeyIdx = 0 to column annotation.
                    Annotation banyanShardingKeyAnnotation = new Annotation(BanyanDB.ShardingKey.class.getName(), constPool);
                    banyanShardingKeyAnnotation.addMemberValue("index", new IntegerMemberValue(constPool, 0));
                }
                annotationsAttribute.addAnnotation(columnAnnotation);
                // 给属性添加 @Column 注解
                newField.getFieldInfo().addAttribute(annotationsAttribute);
            } catch (CannotCompileException e) {
                log.error(
                    "Can't add field(including set/get) " + field.getFieldName() + " in " + className + ".", e);
                throw new OALCompileException(e.getMessage(), e);
            }
        }

        /**
         * Generate methods
         * 根据 freemarker 模版 生成方法
         */
        for (String method : METRICS_CLASS_METHODS) {
            StringWriter methodEntity = new StringWriter();
            try {
                configuration.getTemplate("metrics/" + method + ".ftl").process(metricsStmt, methodEntity);
                metricsClass.addMethod(CtNewMethod.make(methodEntity.toString(), metricsClass));
            } catch (Exception e) {
                log.error("Can't generate method " + method + " for " + className + ".", e);
                throw new OALCompileException(e.getMessage(), e);
            }
        }

        /**
         * Add following annotation to the metrics class
         * 给 Metrics 类添加 @Stream 注解
         * at Stream(name = "${tableName}", scopeId = ${sourceScopeId}, builder = ${metricsName}Metrics.Builder.class, processor = MetricsStreamProcessor.class)
         */
        AnnotationsAttribute annotationsAttribute = new AnnotationsAttribute(
            constPool, AnnotationsAttribute.visibleTag);
        Annotation streamAnnotation = new Annotation(Stream.class.getName(), constPool);
        streamAnnotation.addMemberValue("name", new StringMemberValue(metricsStmt.getTableName(), constPool));
        streamAnnotation.addMemberValue(
            "scopeId", new IntegerMemberValue(constPool, metricsStmt.getFrom().getSourceScopeId()));
        streamAnnotation.addMemberValue(
            "builder", new ClassMemberValue(metricsBuilderClassName(metricsStmt, true), constPool));
        streamAnnotation.addMemberValue("processor", new ClassMemberValue(METRICS_STREAM_PROCESSOR, constPool));

        annotationsAttribute.addAnnotation(streamAnnotation);
        metricsClassClassFile.addAttribute(annotationsAttribute);

        Class targetClass;
        try {
            // 将生成的 Metrics 类注入到类加载器生成目标类
            if (SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8)) {
                targetClass = metricsClass.toClass(currentClassLoader, null);
            } else {
                targetClass = metricsClass.toClass(MetricClassPackageHolder.class);
            }
        } catch (CannotCompileException e) {
            log.error("Can't compile/load " + className + ".", e);
            throw new OALCompileException(e.getMessage(), e);
        }

        log.debug("Generate metrics class, " + metricsClass.getName());
        // 将 Metrics 类写入到文件，debug 才模式生效
        writeGeneratedFile(metricsClass, "metrics");

        return targetClass;
    }

    /**
     * Generate metrics class builder and inject it to classloader
     */
    private void generateMetricsBuilderClass(AnalysisResult metricsStmt) throws OALCompileException {
        String className = metricsBuilderClassName(metricsStmt, false);
        CtClass metricsBuilderClass = classPool.makeClass(metricsBuilderClassName(metricsStmt, true));
        try {
            // 实现 StorageBuilder 接口
            metricsBuilderClass.addInterface(classPool.get(storageBuilderFactory.builderTemplate().getSuperClass()));
        } catch (NotFoundException e) {
            log.error("Can't find StorageBuilder interface for " + className + ".", e);
            throw new OALCompileException(e.getMessage(), e);
        }

        /**
         * Create empty construct
         */
        try {
            CtConstructor defaultConstructor = CtNewConstructor.make(
                "public " + className + "() {}", metricsBuilderClass);
            metricsBuilderClass.addConstructor(defaultConstructor);
        } catch (CannotCompileException e) {
            log.error("Can't add empty constructor in " + className + ".", e);
            throw new OALCompileException(e.getMessage(), e);
        }

        /**
         * Generate methods
         */
        for (String method : METRICS_BUILDER_CLASS_METHODS) {
            StringWriter methodEntity = new StringWriter();
            try {
                configuration
                    .getTemplate(storageBuilderFactory.builderTemplate().getTemplatePath() + "/" + method + ".ftl")
                    .process(metricsStmt, methodEntity);
                metricsBuilderClass.addMethod(CtNewMethod.make(methodEntity.toString(), metricsBuilderClass));
            } catch (Exception e) {
                log.error("Can't generate method " + method + " for " + className + ".", e);
                throw new OALCompileException(e.getMessage(), e);
            }
        }

        try {
            if (SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8)) {
                metricsBuilderClass.toClass(currentClassLoader, null);
            } else {
                metricsBuilderClass.toClass(MetricBuilderClassPackageHolder.class);
            }
        } catch (CannotCompileException e) {
            log.error("Can't compile/load " + className + ".", e);
            throw new OALCompileException(e.getMessage(), e);
        }

        writeGeneratedFile(metricsBuilderClass, "metrics/builder");
    }

    /**
     * Generate SourceDispatcher class and inject it to classloader
     */
    private Class generateDispatcherClass(String scopeName,
                                          DispatcherContext dispatcherContext) throws OALCompileException {
        // ServiceInstanceJVMGC -> ServiceInstanceJVMGCDispatcher
        String className = dispatcherClassName(scopeName, false);
        CtClass dispatcherClass = classPool.makeClass(dispatcherClassName(scopeName, true));
        try {
            CtClass dispatcherInterface = classPool.get(DISPATCHER_INTERFACE);
            // 实现 SourceDispatcher 接口
            dispatcherClass.addInterface(dispatcherInterface);

            /**
             * Set generic signature
             * 设置泛型签名 SourceDispatcher<ServiceInstanceJVMGC>
             */
            String sourceClassName = oalDefine.getSourcePackage() + dispatcherContext.getSource();
            SignatureAttribute.ClassSignature dispatcherSignature =
                new SignatureAttribute.ClassSignature(
                    null, null,
                    // Set interface and its generic params
                    new SignatureAttribute.ClassType[] {
                        new SignatureAttribute.ClassType(
                            SourceDispatcher.class
                                .getCanonicalName(),
                            new SignatureAttribute.TypeArgument[] {
                                new SignatureAttribute.TypeArgument(
                                    new SignatureAttribute.ClassType(
                                        sourceClassName))
                            }
                        )
                    }
                );

            dispatcherClass.setGenericSignature(dispatcherSignature.encode());
        } catch (NotFoundException e) {
            log.error("Can't find Dispatcher interface for " + className + ".", e);
            throw new OALCompileException(e.getMessage(), e);
        }

        /**
         * Generate methods
         * 每一个 metrics 都会对应一个 do${metricsName} 方法
         *
         * instance_jvm_old_gc_time = from(ServiceInstanceJVMGC.time).filter(phase == GCPhase.OLD).sum();
         * instance_jvm_old_gc_time -> doInstanceJvmOldGcTime
         * doXxx 将 Metrics 交给 MetricsStreamProcessor 处理
         */
        for (AnalysisResult dispatcherContextMetric : dispatcherContext.getMetrics()) {
            StringWriter methodEntity = new StringWriter();
            try {
                configuration.getTemplate("dispatcher/doMetrics.ftl").process(dispatcherContextMetric, methodEntity);
                dispatcherClass.addMethod(CtNewMethod.make(methodEntity.toString(), dispatcherClass));
            } catch (Exception e) {
                log.error(
                    "Can't generate method do" + dispatcherContextMetric.getMetricsName() + " for " + className + ".",
                    e
                );
                log.error("Method body as following" + System.lineSeparator() + "{}", methodEntity);
                throw new OALCompileException(e.getMessage(), e);
            }
        }

        try {
            // 生成 dispatch 方法（SourceDispatcher 接口）
            StringWriter methodEntity = new StringWriter();
            configuration.getTemplate("dispatcher/dispatch.ftl").process(dispatcherContext, methodEntity);
            dispatcherClass.addMethod(CtNewMethod.make(methodEntity.toString(), dispatcherClass));
        } catch (Exception e) {
            log.error("Can't generate method dispatch for " + className + ".", e);
            throw new OALCompileException(e.getMessage(), e);
        }

        Class targetClass;
        try {
            if (SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8)) {
                targetClass = dispatcherClass.toClass(currentClassLoader, null);
            } else {
                targetClass = dispatcherClass.toClass(DispatcherClassPackageHolder.class);
            }
        } catch (CannotCompileException e) {
            log.error("Can't compile/load " + className + ".", e);
            throw new OALCompileException(e.getMessage(), e);
        }

        writeGeneratedFile(dispatcherClass, "dispatcher");
        return targetClass;
    }

    private String metricsClassName(AnalysisResult metricsStmt, boolean fullName) {
        return (fullName ? oalDefine.getDynamicMetricsClassPackage() : "") + metricsStmt.getMetricsName() + "Metrics";
    }

    private String metricsBuilderClassName(AnalysisResult metricsStmt, boolean fullName) {
        return (fullName ? oalDefine.getDynamicMetricsBuilderClassPackage() : "") + metricsStmt.getMetricsName() + "MetricsBuilder";
    }

    private String dispatcherClassName(String scopeName, boolean fullName) {
        return (fullName ? oalDefine.getDynamicDispatcherClassPackage() : "") + scopeName + "Dispatcher";
    }

    private void buildDispatcherContext(AnalysisResult metricsStmt) {
        String sourceName = metricsStmt.getFrom().getSourceName();
        // instance_jvm_old_gc_time = from(ServiceInstanceJVMGC.time).filter(phase == GCPhase.OLD).sum();
        // sourceName -> ServiceInstanceJVMGC
        DispatcherContext context = allDispatcherContext.getAllContext().computeIfAbsent(sourceName, name -> {
            DispatcherContext absent = new DispatcherContext();
            absent.setSourcePackage(oalDefine.getSourcePackage());
            absent.setSource(name);
            absent.setPackageName(name.toLowerCase());
            return absent;
        });
        metricsStmt.setMetricsClassPackage(oalDefine.getDynamicMetricsClassPackage());
        metricsStmt.setSourcePackage(oalDefine.getSourcePackage());
        // 将同一个 sourceName 的 metrics 放在一个 DispatcherContext 中
        context.getMetrics().add(metricsStmt);
    }

    private void prepareRTTempFolder() {
        if (openEngineDebug) {
            // server-core-xxx.jar 所在目录
            File workPath = WorkPath.getPath();
            File folder = new File(workPath.getParentFile(), "oal-rt/");
            if (folder.exists()) {
                try {
                    FileUtils.deleteDirectory(folder);
                } catch (IOException e) {
                    log.warn("Can't delete " + folder.getAbsolutePath() + " temp folder.", e);
                }
            }
            folder.mkdirs();
        }
    }

    private void writeGeneratedFile(CtClass metricsClass, String type) throws OALCompileException {
        if (openEngineDebug) {
            String className = metricsClass.getSimpleName();
            DataOutputStream printWriter = null;
            try {
                File workPath = WorkPath.getPath();
                File folder = new File(workPath.getParentFile(), "oal-rt/" + type);
                if (!folder.exists()) {
                    folder.mkdirs();
                }
                File file = new File(folder, className + ".class");
                if (file.exists()) {
                    file.delete();
                }
                file.createNewFile();

                printWriter = new DataOutputStream(new FileOutputStream(file));
                metricsClass.toBytecode(printWriter);
                printWriter.flush();
            } catch (IOException e) {
                log.warn("Can't create " + className + ".txt, ignore.", e);
                return;
            } catch (CannotCompileException e) {
                log.warn("Can't compile " + className + ".class(should not happen), ignore.", e);
                return;
            } finally {
                if (printWriter != null) {
                    try {
                        printWriter.close();
                    } catch (IOException e) {

                    }
                }
            }
        }
    }
}
