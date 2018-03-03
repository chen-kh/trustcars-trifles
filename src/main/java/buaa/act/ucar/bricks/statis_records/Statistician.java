package buaa.act.ucar.bricks.statis_records;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class Statistician {
	public static Map<String, Map<String, Integer>> statisMap = new HashMap<String, Map<String, Integer>>();

	public static synchronized void addCountMap(Map<String, Map<String, Integer>> countMap) {
		Iterator<Entry<String, Map<String, Integer>>> iterator = countMap.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, Map<String, Integer>> entry = iterator.next();
			if(statisMap.containsKey(entry.getKey())){
				Map<String, Integer> map = statisMap.get(entry.getKey());
				Map<String, Integer> map2 = entry.getValue();
				Iterator<Entry<String, Integer>> it = map2.entrySet().iterator();
				while(it.hasNext()){
					Entry<String, Integer> entry2 = it.next();
					String key = entry2.getKey();
					int statis = map.get(key) + map2.get(key);
					map.put(key, statis);
				}
				statisMap.put(entry.getKey(), map);
			}else {
				statisMap.put(entry.getKey(), countMap.get(entry.getKey()));
			}
		}
	}
}
