package generators;

import java.util.Date;

public class DynamicIntegerGenerator implements IntegerGenerator {
	public int[] basevalue;
	public int[] delta;
			
	public DynamicIntegerGenerator(int[] basevalue, int[] delta) {
		super();
		this.basevalue = basevalue;
		this.delta = delta;
	}


	@Override
	public int generateValue() {
		if(basevalue.length>3){
			if(delta.length>3){
				@SuppressWarnings("deprecation")
				int hnow	=	(new Date()).getHours();
				int aValue	=	(int)((Math.random()*delta[hnow/6])+basevalue[hnow/6]);
				return aValue;
			}
		}
		// TODO Auto-generated method stub
		return 0;
	}

}
