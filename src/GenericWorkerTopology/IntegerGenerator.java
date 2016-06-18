package GenericWorkerTopology;

public class IntegerGenerator {
	public static int basevalue	=	35;
	public static int delta		=	5;
	
	public static int generateValue(){
		int value	=	(int)((Math.random()*delta)+basevalue);
		return value;
	}
}
