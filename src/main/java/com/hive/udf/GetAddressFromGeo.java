package com.hive.udf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;


@Description(extended = "", name = "", value = "")
public class GetAddressFromGeo extends GenericUDF {

	

	private final static String URL_MAP_GOOGLE="https://maps.googleapis.com/maps/api/geocode/json?key=YOUR_KEY&result_type=street_address&location_type=ROOFTOP&latlng=";
	
	
	private ObjectInspectorConverters.Converter[] converters;
	
	/**

	 * @param s
	 * @return
	 */
	
	private String getAddressFromLatLon(Double latitud,Double longitu) {
				
		
		
		String sUrl=URL_MAP_GOOGLE+latitud.toString()+","+longitu.toString();
		
		try {
			URL url=new URL(sUrl);
			HttpURLConnection httpConnection= (HttpURLConnection)url.openConnection();
			httpConnection.setRequestMethod("GET");
			BufferedReader buff=new BufferedReader(new InputStreamReader(httpConnection.getInputStream()));
			String line=null;
			while((line=buff.readLine())!=null){				
				int ini=line.indexOf("formatted_address");				
				String result=null;
				if(ini>-1){
					result=line.substring(ini+"formatted_address".length()+1);
					return result;
				}				
			}
			
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {

		assert(arguments.length==2);
		
		assert(arguments[0].getCategory()==Category.PRIMITIVE);
		assert(arguments[1].getCategory()==Category.PRIMITIVE);
		
		for (ObjectInspector objectInspector : arguments) {

			PrimitiveCategory primitiveCategory= ((PrimitiveObjectInspector)objectInspector).getPrimitiveCategory();
			
				if(primitiveCategory!=PrimitiveCategory.FLOAT){
					throw new UDFArgumentTypeException(0, "Un FLOAT era esperado CARAJO !!!");					
				}		
		}
		
		converters=new ObjectInspectorConverters.Converter[arguments.length];
		for(int i=0;i<arguments.length;i++){
			converters[i]=ObjectInspectorConverters.getConverter(arguments[i], PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
		}
		
		return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		
		assert(arguments.length==2);
		
		if(arguments[0].get()==null || arguments[1].get()==null){
			return null;
		}
		
		FloatWritable input1=(FloatWritable) converters[0].convert(arguments[0].get());
		FloatWritable input2=(FloatWritable) converters[1].convert(arguments[1].get());
	
		String address=getAddressFromLatLon(Double.valueOf(input1.toString()), Double.valueOf(input2.toString()));
		if(address==null){
			address="no address";
		}
		return new Text(address);
	}
	

	@Override
	public String getDisplayString(String[] children) {
		return new String("geo_address");
	}

	
	
}
