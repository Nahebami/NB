import os.path
def line_compare(x,y):
	if x == y:
		return (True,x,y)
	else:
		return (False,x,y)

def print_output(x):
        if x[0]:
                return x[1].strip() + ' equals ' + x[2].strip()
        else:
                return x[1].strip() + ' not equals ' + x[2].strip()

def file_comparator(file1,file2):
	try:
		if os.path.isfile(file1) and os.path.isfile(file2):
			pr_out = lambda x: x[1].strip() + ' equals ' + x[2].strip() if x[0] else x[1].strip() + ' not equals ' + x[2].strip()
			lin_comp = lambda x,y: (True,x,y) if x == y else  (False,x,y)
			file1_,file2_ = open(file1,'r').readlines(),open(file2,'r').readlines()
			if file1_ and len(file2_) > 0:
				return list(map(pr_out,list(map(lin_comp,file1_,file2_))))
			else:
				return []
	except:
		print('Input variable is not a file')
	finally:
		print('The Try comparison is done.')
		

