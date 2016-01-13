package com.lsu.shim;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.DecompositionSolver;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.linear.SingularValueDecomposition;

import com.lsu.objects.Query;
import com.lsu.test.ClientQueryExampleThread;

public class Verifier {

	public static int[] solver(double[][] equations){
		RealMatrix coefficients =
			    new Array2DRowRealMatrix(equations, false);
		DecompositionSolver solver = new SingularValueDecomposition(coefficients).getSolver();
		double[] contArr = new double[DependencyChecker.serializeList.size()-1];
		Arrays.fill(contArr, ClientQueryExampleThread.replicas +1);
		RealVector constants = new ArrayRealVector(contArr, false);
		RealVector solution = solver.solve(constants);
		int i =0;
		int[] entry = new int[DependencyChecker.serializeList.size()];
		while(i<entry.length-1)//solution.getEntry(i)!=0)
		{
			entry[i] = (int) solution.getEntry(i);
			//System.out.println("***entry[i]:="+entry[i]);
			i++;
		}
		return entry;
		//double entry1 = solution.getEntry(1);
	}
	
	public static double[][] formEquations() {
		// TODO Auto-generated method stub
		double[][] eqns = new double[DependencyChecker.serializeList.size()-1][DependencyChecker.serializeList.size()];
		//System.out.println("***222 DependencyChecker.serializeList:=="+DependencyChecker.serializeList.size());
		int i=0, index = -1, index1 = -1;
		boolean flag = false;
		while(i<DependencyChecker.serializeList.size()-1){
			index = Integer.parseInt(DependencyChecker.serializeList.get(i).getIndex());
			index1 = Integer.parseInt(DependencyChecker.serializeList.get(i+1).getIndex());
			for(int k=0;k<DependencyChecker.serializeList.size()-1;k++)
			{
				if(k==index || k==index1)
				{
					eqns[i][k] = 1;
				}
			}
			i++;
		}
		/*while(i<DependencyChecker.serializeList.size()-1)
		{
			if(i==0)
			{
				index = Integer.parseInt(DependencyChecker.serializeList.get(i).getIndex());
				index1 = Integer.parseInt(DependencyChecker.serializeList.get(i+1).getIndex());
				eqns[i][index]=1;
				eqns[i][index1]=1;
				i++;
			}
			else
			{
				index = index1;
				index1 = Integer.parseInt(DependencyChecker.serializeList.get(i).getIndex());
				eqns[i][index]=1;
				eqns[i][index1]=1;
				
			}
			//eqns[index][index1]=1;
			i++;
		}*/
		
		/*for(int i=0;i<queryList.size();i++)
		{
			
		}*/
		return eqns;
	}
	
	public static void chooseConsistencyLevels() {
		// TODO Auto-generated method stub\
		String cLevel = "ALL";
		//System.out.println("***dependencyChecker.getSerializeList().size():="+dependencyChecker.getSerializeList().size());
		double[][] eqns =  formEquations();
		//System.out.println("***1111 eqns.length:=="+eqns.length);
		int[] replicas = solver(eqns);
		List<String> queryList = DependencyChecker.queryList;
		for(int i=0;i<DependencyChecker.serializeList.size();i++)
		{
			//cLevel = Verifier.chooseConsistencyLevel();
			/*if(i>0)
				replicas = ClientQueryExampleThread.replicas - replicas+1;
			else
				replicas = 1;*/
			//cLevel = DependencyChecker.serializeList.get(i).getConsistencyLevel();
			//if(consistencyReplicas(cLevel)==1)
			//replicas = consistencyReplicas(cLevel);
			DependencyChecker.serializeList.get(i).setConsistencyLevel(consistencyReplicas(replicas[i]));
			//System.out.println("***DependencyChecker.serializeList.get(i).getConsistencyLevel():="+i+" :i:="+DependencyChecker.serializeList.get(i).getConsistencyLevel());
		}
		/*for(int i=0;i<queryList.size();i++)
		{
			
		}*/
	}
	
	public static String consistencyReplicas(int replicas){
		String cLevel = "ALL";
		if(replicas == ClientQueryExampleThread.replicas/2)
			cLevel = "QUORUM";
		else if(replicas == 2)
			cLevel = "TWO";
		else if(replicas == ClientQueryExampleThread.replicas)
			cLevel = "ALL";
		else if(replicas == 1)
			cLevel = "ONE";
		return cLevel;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
