package edu.buffalo.cse.cse486586.groupmessenger2;

import java.util.Comparator;

public class MessageComparator implements Comparator<Message> {

        @Override
        public int compare(Message u,Message v) {

            if (u.getSeqNo() < v.getSeqNo())
                return -1;
            else if (u.getSeqNo() > v.getSeqNo())
                return 1;
            return 0;
        }

        /*if (u.getSeqNo() < v.getSeqNo())
            return 1;
        else if (s1.cgpa > s2.cgpa)
            return -1;
        return 0;*/
    }

