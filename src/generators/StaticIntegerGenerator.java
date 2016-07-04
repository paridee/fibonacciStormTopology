package generators;

public class StaticIntegerGenerator implements IntegerGenerator {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4106254709468416303L;
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

	@Override
	public int getBase() {
		return this.basevalue;
	}

	@Override
	public int getDelta() {
		// TODO Auto-generated method stub
		return this.delta;
	}
}
