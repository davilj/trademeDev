package trademe.converters;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import trademe.converters.spark.Transaction;

import com.fasterxml.jackson.databind.module.SimpleAbstractTypeResolver;
import com.google.gson.Gson;

public class SparkToD3jsTreeMapConverter implements IConverter {
	Gson gson = new Gson();
	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	
	

	@Override
	public ConverterData convert(List<String> lines) {
		Node d3TreeMap = new Node();
		List<String> treeMapData = lines;
		Collections.sort(treeMapData);
		String name=null;
		
		for (String line : treeMapData) {
			Transaction transaction = gson.fromJson(line, Transaction.class);
			if (name==null) 
				name = transaction.getDayDate();
			
			Node node = new Node();
			node.setName(transaction.getCat());
			node.setAmount(transaction.getAmount());
			node.setValue(transaction.getValue());
			d3TreeMap.addNode(node);
		}
		d3TreeMap.setName(name);
		String json = gson.toJson(d3TreeMap);
		
		return new ConverterData(name, json);
	}
	
	
	
	

	static class Node {
		private String name;
		private List<Node>  children = new ArrayList<Node>();
		private Long value;
		private Long amount;
		
		public String getName() {
			return name;
		}
		
		public void addNode(Node node) {
			int index = node.name.indexOf('|');
			if (index==-1) {
				//no more parents
				this.children.add(node);
			} else {
				String newName = node.name.substring(index+1);
				String parentName = node.name.substring(0, index);
				node.setName(newName);
				for (Node child : this.children) {
					if (child.getName().equals(parentName)) {
						child.addNode(node);
						return;
					}
				}
				
				Node parentNode = new Node();
				parentNode.setName(parentName);
				this.children.add(parentNode);
				parentNode.addNode(node);
				
			}
			
		}
		
		public void setValue(Long value) {
			this.value = value;
			
		}
		
		public void setAmount(Long amount) {
			this.amount = amount;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		
		
		public boolean isLeave() {
			return this.children==null || this.children.size()==0;
		}
			
	}

}
