import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class program {

	public interface Predicate<T> { boolean apply(T type); }
	
	private static String memberString="SessionGMTStartTime";
	private static String delimiter = "|";
	private static String schema = "C:\\workspace\\RawDataFilter\\src\\Session.xml";
	private static String columnField = "name";
	private static String dataFile = "C:\\workspace\\RawDataFilter\\src\\text.txt";
	
	private static ArrayList<String[]> readLines(String file, String delimiter) {
		
		ArrayList<String[]> resultArrayList = new ArrayList<String[]>();
		
		BufferedReader br = null;

		try {

			String sCurrentLine;

			br = new BufferedReader(new FileReader(file));

			while ((sCurrentLine = br.readLine()) != null) {
				String[] line = sCurrentLine.split(Pattern.quote(delimiter));
				resultArrayList.add(line);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return resultArrayList;

	}
	public static ArrayList<String> readSchemaColums(String fileScema, String field) {
		ArrayList<String> data=new ArrayList<String>();;
        Document dom;
        // Make an  instance of the DocumentBuilderFactory
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        NodeList nl;
        try {
            // use the factory to take an instance of the document builder
            DocumentBuilder db = dbf.newDocumentBuilder();
            // parse using the builder to get the DOM mapping of the    
            // XML file
            dom = db.parse(fileScema);
            Document document = db.parse(new File(fileScema));
            nl = document.getElementsByTagName("member");
            NodeList del=document.getElementsByTagName("DataSchema");
            delimiter=del.item(0).getAttributes().getNamedItem("delimiter").getNodeValue();
            for (int k=0;k<nl.getLength();k++) {
        		data.add(nl.item(k).getAttributes().getNamedItem(field).getNodeValue());
            }
        } catch (ParserConfigurationException pce) {
            System.out.println(pce.getMessage());
        } catch (SAXException se) {
            System.out.println(se.getMessage());
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        }
        return data;
	}	
	public static <T> Collection<T> filter(Collection<T> col, Predicate<T> predicate) {
		  Collection<T> result = new ArrayList<T>();
		  for (T element: col) {
		    if (predicate.apply(element)) {
		      result.add(element);
		    }
		  }
		  return result;
	}
	
	public static void main(String[] args) {
		ArrayList<RawData> data=new ArrayList<RawData>();
		
		ArrayList<String> schemaArrayList = new ArrayList<String>();		
		
		schemaArrayList = readSchemaColums(schema, columnField);
		
		ArrayList<String[]> linesData=readLines(dataFile, delimiter);
		
		RawData tempRawData;
		for(String[] s:linesData) {
			tempRawData = new RawData();
			for (int k=0;k<s.length;k++) {
				tempRawData.addMember( schemaArrayList.get(k), s[k]);
			}
			data.add(tempRawData);
		}
		final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		
		class RawDataFilterPredicate implements Predicate<RawData>
		{
			  public boolean apply(RawData inputRawData) {
				  	boolean isApplicable=false;
				  	long mTimeLong=0;
					    try {
					    	mTimeLong=formatter.parse(inputRawData.getMember(memberString)).getTime();
						} catch (ParseException e) {
							e.printStackTrace();
						}
						isApplicable = (mTimeLong<Long.parseLong("1459574738000"));
//						isApplicable = (mTimeLong>259200000)&&
//								inputRawData.getMember("SessionDuration").contains("159")&&
//								inputRawData.getMember("SessionID").contains("123413459");

					return isApplicable;
				    
				  }
		}
		
		Predicate<RawData> validDataPredicate = new  RawDataFilterPredicate();
			 
		Collection<RawData> result = filter(data, validDataPredicate);
		for(RawData p : result) {
			System.out.println(p.getMember(memberString));
		}
		
	}
}
