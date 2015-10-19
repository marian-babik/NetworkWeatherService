package edu.uchicago.networkweather;

public class MappingPair<site, vo> {
    public  site first;
    public  vo second;

    public MappingPair(site first, vo second) {
    	super();
    	this.first = first;
    	this.second = second;
    }
    public site getSite() {
    	return first;
    }
    public vo getVO() {
    	return second;
    }

}
