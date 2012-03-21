package uk.ac.ed.inf.nmr.mapreduce;




public class JobTrigger {

	protected String condition;
	
	public JobTrigger(){
		condition = "default";
	}
	
	public void setCondition(String condition){
		this.condition = condition;
	}
	
	public String getCondition(){
		return this.condition;
	}
	
}
