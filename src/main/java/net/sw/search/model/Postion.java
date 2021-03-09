package net.sw.search.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;

/**
 *
 */
public class Postion {
    /**
     * 主键
     */
    private String id;
    /**
     * 公司名称
     */
    private String companyName;
    /**
     * 职位诱惑
     */
    private String positionAdvantage;
    /**
     * 薪资
     */
    private String salary;
    /**
     * 薪资下限
     */
    private int salaryMin;
    /**
     * 薪资上限
     */
    private int salaryMax;
    /**
     * 学历
     */
    private String education;
    /**
     * 工作年限
     */
    private String workYear;
    /**
     * 发布时间
     */
    private String publishTime;
    /**
     * 工作城市
     */
    private String city;
    /**
     * 工作地点
     */
    private String workAddress;
    /**
     * 创建时间
     */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date createTime;
    /**
     * 工作模式
     */
    private String jobNature;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getPositionAdvantage() {
        return positionAdvantage;
    }

    public void setPositionAdvantage(String positionAdvantage) {
        this.positionAdvantage = positionAdvantage;
    }

    public String getSalary() {
        return salary;
    }

    public void setSalary(String salary) {
        this.salary = salary;
    }

    public int getSalaryMin() {
        return salaryMin;
    }

    public void setSalaryMin(int salaryMin) {
        this.salaryMin = salaryMin;
    }

    public int getSalaryMax() {
        return salaryMax;
    }

    public void setSalaryMax(int salaryMax) {
        this.salaryMax = salaryMax;
    }

    public String getEducation() {
        return education;
    }

    public void setEducation(String education) {
        this.education = education;
    }

    public String getWorkYear() {
        return workYear;
    }

    public void setWorkYear(String workYear) {
        this.workYear = workYear;
    }

    public String getPublishTime() {
        return publishTime;
    }

    public void setPublishTime(String publishTime) {
        this.publishTime = publishTime;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getWorkAddress() {
        return workAddress;
    }

    public void setWorkAddress(String workAddress) {
        this.workAddress = workAddress;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getJobNature() {
        return jobNature;
    }

    public void setJobNature(String jobNature) {
        this.jobNature = jobNature;
    }
}
