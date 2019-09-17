package edu.buffalo.cse.cse486586.groupmessenger2;

import java.util.Comparator;

public class Message implements Comparable<Message>{

    public String portNumber;
    public String msg;
    public Float seqNo;
    public boolean isDeliverable;
    public boolean updated;

    public boolean isUpdated() {
        return updated;
    }

    public void setUpdated(boolean updated) {
        this.updated = updated;
    }

    public Message (String msg, Float seq,String port){
        this.setMsg(msg);
        this.setSeqNo(seq);
        this.setPortNumber(port);

    }

    public boolean isDeliverable() {
        return isDeliverable;
    }

    public void setDeliverable(boolean deliverable) {
        isDeliverable = deliverable;
    }

    public String getPortNumber() {
        return portNumber;
    }

    public void setPortNumber(String portNumber) {
        this.portNumber = portNumber;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Float getSeqNo() {
        return seqNo;
    }

    public void setSeqNo(Float seqNo) {
        this.seqNo = seqNo;
    }

    @Override
    public int compareTo(Message u) {

        return this.seqNo.compareTo(u.getSeqNo());
    }

    }
