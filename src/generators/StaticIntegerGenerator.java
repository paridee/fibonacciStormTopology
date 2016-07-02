package generators;

public class StaticIntegerGenerator implements IntegerGenerator {
	public int basevalue	=	31;
	public int delta		=	5;
		
	public StaticIntegerGenerator(int basevalue, int delta) {
		super();
		this.basevalue = basevalue;
		this.delta = delta;
	}

	public int generateValue(){
		int value	=	(int)((Math.random()*delta)+basevalue);
		return value;
	}
}
