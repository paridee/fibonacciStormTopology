package generators;

import java.io.Serializable;

public interface IntegerGenerator extends Serializable {
	 int generateValue();
	 int getBase();
	 int getDelta();
}
