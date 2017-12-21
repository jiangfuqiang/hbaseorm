package com.weidian.proxy.hbase.common;

import com.weidian.proxy.hbase.annotation.ColName;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jiang on 17/10/28.
 */
public abstract class BaseHbaseDao<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseHbaseDao.class);
    private static final int length = 8;
    private static final int int_length = 4;
    private static final int short_length = 2;
    private static final int offset = 0;

    private static Map<String, Method> methodMap = new HashMap<String, Method>();
    private static Map<String, Field[]> fieldMap = new HashMap<String, Field[]>();

    protected Object newInstanceObject(Class clazz) {
        Object object = null;
        try {
            object = clazz.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return object;
    }

    protected Object reflectT(Object object, Class clazz, Cell cell,byte[] value, String colName) throws Exception{


            try {
                //把列名转为方法驼峰名
                String methodName = transferMethod(colName);

                String key = clazz.getName() + "_" + methodName;
                Field[] fields = null;
                if(fieldMap.containsKey(key)) {
                    fields = fieldMap.get(key);
                } else {
                    fields = clazz.getDeclaredFields();
                    fieldMap.put(key, fields);
                }
                for(Field field : fields) {
                    Annotation[] annotations = field.getDeclaredAnnotations();
                    if(annotations == null || annotations.length == 0) {
                        continue;
                    }
                    boolean isSet = false;
                    for(Annotation annotation : annotations) {
                        if(annotation instanceof ColName) {
                            //如果有注解，则用注解中指定的名字匹配hbase的列名
                            if(colName.equals(((ColName) annotation).name())) {
                                String fieldName = field.getName();
                                methodName = transferFieldName(fieldName);
                                isSet = true;
                                break;
                            }
                        }
                    }
                    if(isSet) {
                        break;
                    }
                }


                Method method = null;

                if(methodMap.containsKey(key)) {
                    method = methodMap.get(key);
                } else {
                    method = getMethod(clazz, methodName);
                    if(method != null) {
                        methodMap.put(key, method);
                    }
                }
                if(method == null) {
                    LOGGER.warn("not found this method : " + methodName);
                    return object;
                }

                Class[] parameterTypes = method.getParameterTypes();

                Object[] values = new Object[parameterTypes.length];
                if(parameterTypes.length > 0) {
                    int index = 0;
                    for(Class ctClass : parameterTypes) {
                        try {
                            //将字节数组值转换为普通类型值
                            values[index] = changeMethodValue(ctClass.getSimpleName(),cell,value);
                        } catch (Exception e) {
                            throw new IllegalArgumentException("change value error: " + colName + " " + clazz.getName(),e);
                        }
                        index += 1;
                    }
                }

                try {
                    method.invoke(object, values);
                } catch (InvocationTargetException e) {
                    LOGGER.error("colName=" + colName + " " + key + " " + values.toString(),e);
                    throw new IllegalArgumentException(e.getMessage(),e);
                }

            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(e.getMessage(),e);
            }

        return object;
    }

    /**
     * 获取类或者父类的method
     * @param clazz
     * @param methodName
     * @return
     */
    private Method getMethod(Class clazz,String methodName) {
        Method[] methods = clazz.getDeclaredMethods();
        Method method = null;
        for(Method m : methods) {
            String mn = m.getName();
            if(mn.equals(methodName)) {
                method = m;
                break;
            }
        }
        if(method == null) {
            Class superClass = clazz.getSuperclass();
            if(superClass != null) {
                method = getMethod(superClass,methodName);
            }
        }
        return method;
    }

    /**
     * 转换方法参数类型
     * @param clazzName
     * @param cell
     * @return
     */
    private Object changeMethodValue(String clazzName, Cell cell,byte[] value) throws Exception{
        clazzName = clazzName.toLowerCase();
        String iv = new String(value,"UTF-8");
        switch (clazzName) {
            case "string":
                if(cell != null) {
                    return Bytes.toString(CellUtil.cloneValue(cell));
                } else {
                    return new String(value,"UTF-8");
                }
            case "int":
            case "integer":
                if(cell != null && offset + int_length <= value.length && (iv.indexOf("�") >= 0 || iv.indexOf("\u0000") >= 0)) {
                    return Bytes.toInt(CellUtil.cloneValue(cell));
                } else {
                    if("".equals(iv)) {
                        return null;
                    }
                    return Integer.parseInt(iv);
                }
            case "double":
                if(cell != null && offset + length <= value.length && (iv.indexOf("�") >= 0 || iv.indexOf("\u0000") >= 0)) {
                    return Bytes.toDouble(CellUtil.cloneValue(cell));
                } else {
                    if("".equals(iv)) {
                        return null;
                    }
                    return Double.parseDouble(iv);
                }
            case "float":
                if(cell != null &&  offset + int_length <= value.length && (iv.indexOf("�") >= 0 || iv.indexOf("\u0000") >= 0)) {
                    return Bytes.toFloat(CellUtil.cloneValue(cell));
                } else {
                    if("".equals(iv)) {
                        return null;
                    }
                    return Float.parseFloat(iv);
                }
            case "long":

                if(cell != null  && offset + length <= value.length && (iv.indexOf("�") >= 0 || iv.indexOf("\u0000") >= 0)) {
                    return Bytes.toLong(CellUtil.cloneValue(cell));
                } else {
                    if("".equals(iv)) {
                        return null;
                    }
                    return Long.parseLong(iv);
                }
            case "char":
                if(cell != null  && offset + length <= value.length && (iv.indexOf("�") >= 0 || iv.indexOf("\u0000") >= 0)) {
                    return Bytes.toInt(CellUtil.cloneValue(cell));
                } else {
                    if("".equals(iv)) {
                        return null;
                    }
                    return iv.toString().charAt(0);
                }
            case "short":
                if(cell != null  && offset + short_length <= value.length && (iv.indexOf("�") >= 0 || iv.indexOf("\u0000") >= 0)) {
                    return Bytes.toShort(CellUtil.cloneValue(cell));
                } else {
                    if("".equals(iv)) {
                        return null;
                    }
                    return Short.parseShort(iv);
                }
            case "byte":
                return Bytes.toBytes(new String(CellUtil.cloneValue(cell)));
            case "bigdecimal":
                if(cell != null  && value.length >= 5 && (iv.indexOf("�") >= 0 || iv.indexOf("\u0000") >= 0)) {
                    return new BigDecimal(Bytes.toDouble(CellUtil.cloneValue(cell)));
                } else {
                    if("".equals(iv)) {
                        return new BigDecimal(0);
                    }
                    return BigDecimal.valueOf(Double.parseDouble(iv));
                }

            case "biginteger":
                if(cell != null &&  value.length >= 5 && (iv.indexOf("�") >= 0 || iv.indexOf("\u0000") >= 0)) {
                    return BigInteger.valueOf(Bytes.toLong(CellUtil.cloneValue(cell)));
                } else {
                    if("".equals(iv)) {
                        return new BigInteger("0");
                    }
                    return BigInteger.valueOf(Long.parseLong(iv));
                }

            case "date":
                Date ndate = null;
                String svalue = "";
                if(cell != null && (iv.indexOf("�") >= 0 || iv.indexOf("\u0000") >= 0)) {
                    svalue = Bytes.toString(CellUtil.cloneValue(cell));
                } else {
                    svalue = new String(value,"UTF-8");
                }
                if(svalue.indexOf("-") > 0) {
                    String format = dateFormatValue(svalue);
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
                    try {
                        ndate = simpleDateFormat.parse(svalue);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                } else {
                    if(cell != null && offset + length <= value.length) {
                        ndate = new Date(Bytes.toLong(CellUtil.cloneValue(cell)));
                    } else {
                        ndate = new Date(Long.parseLong(new String(value,"UTF-8")));
                    }
                }
                return ndate;
            case "timestamp":
                if(cell != null && offset + length <= value.length) {
                    return new Timestamp(Bytes.toLong(CellUtil.cloneValue(cell)));
                } else {
                    return new Timestamp(Long.parseLong(new String(value,"UTF-8")));
                }
                default:return value;
        }
    }

    private String dateFormatValue(String value) {
        String[] dateTime = value.split(" ");
        if(dateTime.length == 1) {
            String dt = dateTime[0];
            if(dt.indexOf("-") > 0) {
                String[] dtSp = dt.split("-");
                if(dtSp.length == 2) {
                    return "yyyy-MM";
                }else {
                    return "yyyy-MM-dd";
                }
            } else {
                String[] tsp = dt.split(":");
                if(tsp.length == 2) {
                    return "HH:mm";
                } else {
                    return "HH:mm:ss";
                }
            }
        } else {
            String tsp = dateTime[1];
            String[] tsps = tsp.split(":");
            if(tsps.length == 2) {
                return "yyyy-MM-dd HH:mm";
            } else {
                return "yyyy-MM-dd HH:mm:ss";
            }
        }
    }

    /**
     * 获取方法参数的类型
     * @param clazzName
     * @return
     */
    private Class getMethodParameterClazz(String clazzName) {
        clazzName = clazzName.toLowerCase();
        if(clazzName.equals("string")) {
            return String.class;
        } else if(clazzName.equals("long")) {
            return long.class;
        } else if(clazzName.equals("int")) {
            return int.class;
        } else if(clazzName.equals("double")) {
            return double.class;
        } else if(clazzName.equals("float")) {
            return float.class;
        } else if(clazzName.equals("short")) {
            return short.class;
        } else if(clazzName.equals("byte")) {
            return byte.class;
        } else if(clazzName.equals("bigdecimal")) {
            return BigDecimal.class;
        } else if(clazzName.equals("biginteger")) {
            return BigInteger.class;
        } else if(clazzName.equals("date")) {
            return Date.class;
        } else if(clazzName.equals("timestamp")) {
            return Timestamp.class;
        } else if(clazzName.equals("char")) {
            return char.class;
        }
        return null;
    }

    /**
     * 把hbase的列名转为方法驼峰名
     * @param colName
     * @return
     */
    private String transferMethod(String colName) {
        String[] cns = colName.split("_");
        if(cns.length == 1) {
            return "set" + colName.substring(0,1).toUpperCase()+colName.substring(1);
        }
        StringBuilder methodName = new StringBuilder();
        for(String cn : cns) {
            String cname = cn.substring(0,1).toUpperCase()+cn.substring(1);
            methodName.append(cname);
        }
        return "set"+methodName.toString();
    }

    /**
     * 把hbase的列名转为方法驼峰名
     * @param colName
     * @return
     */
    private String transferFieldName(String colName) {

        return "set" + colName.substring(0,1).toUpperCase()+colName.substring(1);


    }

    protected Object convertToEntity(String rowkey, Class clazz, Result result) {


        Object object = newInstanceObject(clazz);
        if(object == null) {
            LOGGER.error("can't new instance object: " + clazz);
            return object;
        }

        List<Cell> cells = result.listCells();
        if(cells.isEmpty()) {
            LOGGER.error("not found columns: " + rowkey);
            return null;
        }

        try {
            reflectT(object, clazz, null, Bytes.toBytes(rowkey), "rowkey");
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
        }
        for(Cell cell : cells) {
            byte[] value = cell.getValue();
            String qualifier = Bytes.toString(cell.getQualifier());
            try {
                reflectT(object,clazz,cell,value,qualifier);
            } catch (Exception e) {
                LOGGER.error(e.getMessage() + " " + qualifier + " " + rowkey,e);
            }
        }
        return object;
    }

    public Filter getOutFilter(Class filterClazz, String columnFamily, String quafilier, CompareFilter.CompareOp compareOp,String value) {

        if(SingleColumnValueFilter.class == filterClazz) {
            BinaryComparator binaryComparator = new BinaryComparator(Bytes.toBytes(value));
            return new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(quafilier),compareOp,binaryComparator);
        }
        return null;

    }
}
