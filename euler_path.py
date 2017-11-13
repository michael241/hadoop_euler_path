#imports mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep

class MR_program(MRJob):
	def mapper_1 (self, _,line):
		#reads in one line at a time and splits
		line = line.split(" ")
		
		#for each vertex, emits an additional degree to the count of the degrees seen for that vertex
		yield line[0],1
		yield line[1],1

	def reducer_1 (self, key, values):
		#sums the the total degree for each vertex
		#emits if the degrees is even(0) or odd(1)
		yield 'euler',sum(values)%2
		
	def reducer_2 (self, key, values):
		#holds all the results for the checks if each node has odd degree, if the sum is above or equal to 1 there is no euler path
		summmer = sum(values)
		
		#develops if it does or does not have an euler path
		eulerResult = True
		if(summmer>=1):
			eulerResult=False	
			
		#emits result if it does or does not have an euler path
		yield "Euler Result:", eulerResult
		
	def steps(self):
		#orders the mapper and reducers (#1-mapper_1, #2-reducer_1, #3-reducer_2)
		return [
			MRStep(mapper=self.mapper_1, 
			reducer=self.reducer_1),
			MRStep(reducer=self.reducer_2)
		]		
		
if __name__ == '__main__':
	#executes mr job
	MR_program.run()
	
# execute with python euler_path.py g1.txt -q 
