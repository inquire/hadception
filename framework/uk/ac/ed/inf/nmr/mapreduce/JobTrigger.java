package uk.ac.ed.inf.nmr.mapreduce;

/**
 * It encapsulates a string with the state of the Nested MapReduce Job. 
 * 
 * @author Daniel Stanoescu
 *
 */

public class JobTrigger {

	protected String condition;
	
	/**
	 * Default means no nested job wil be started.
	 */
	
	public JobTrigger(){
		condition = "default";
	}
	
	/**
	 * Sets the condition to the name of the NestedJob that will be run
	 * @param condition
	 */
	public void setCondition(String condition){
		this.condition = condition;
	}
	
	/**
	 * Returns the state of the JobTrigger condition
	 * @return name of the nested job or default.
	 */
	public String getCondition(){
		return this.condition;
	}
	
}
