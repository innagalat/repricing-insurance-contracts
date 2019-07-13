import pandas as pd
import numpy as np



# import Tkinter
# from Tkinter import *
# from Tkinter import Tk
# from tkFileDialog import askopenfilename
# from argparse import ArgumentParser

# from Tkinter import Tk, Label, Button

data= pd.read_excel('C:\galati_files\pyscripts\callo-repricing\compare-runs\Compar_table_temp.xlsx')
print data
print data.ix[0,0]


# root = Tk()


# Frame(root, width = 500, height = 500, background = "black")
# height = 7
# width = 6
# for i in range(height): #Rows
#     for j in range(width): #Columns
#     	print (i,j), 
#     	print data.ix[i,j]
#         b = Label(root, text = data.ix[i,j+1], borderwidth = 0, width = 15)
#         b.grid(row=i, column=j)
 


# mainloop()




import Tkinter as tk

class ExampleApp(tk.Tk):
	def __init__(self):
		tk.Tk.__init__(self)
		EntryTable(self,10,2).pack(side= "top")
		
		t = ComparTable(self, 6,6)
		t.pack(side="bottom", fill="x")
		t.set(0,0,"Options Comparison in %")

		

class EntryTable(tk.Frame):
	def __init__(self, parent, rows = 10, columns = 2):
		tk.Frame.__init__(self, parent)
		l1 = tk.Label(self, anchor = "nw", text="Current Sum Insured:", width = 23)
		l1.grid(row = 0, column = 0) #.place(x = , y = ), .pack(side =)

		l2 = tk.Label(self, anchor = "nw", text="Current Age:", width = 23)
		l2.grid(row = 1, column = 0)

		l3 = tk.Label(self, anchor = "nw",  text="Commencement Age:", width = 23)
		l3.grid(row = 3, column = 0)

		l4 = tk.Label(self, anchor = "nw", text="Commencement Sum Insured:", width = 23)
		l4.grid(row = 4, column = 0)


		#define Text Entries for labels 1-4
		si_current_text = tk.IntVar()
		e1 = tk.Entry(self, textvariable = si_current_text)
		e1.grid(row = 0, column = 2)

		age_current_text = tk.IntVar()
		e2 = tk.Entry(self, textvariable = age_current_text)
		e2.grid(row = 1, column = 2)

		si_commenc_text = tk.IntVar()
		e3 =tk.Entry(self, textvariable = si_commenc_text)
		e3.grid(row = 3, column = 2)

		age_commenc_text = tk.IntVar()
		e4 = tk.Entry(self, textvariable = age_commenc_text)
		e4.grid(row = 4, column = 2)




		browse_button = tk.Button(self, text = 'Calculate options comparison',
										command = update)
		browse_button.grid(row=7, column=5)


def update():
		data= pd.read_excel('C:\galati_files\pyscripts\callo-repricing\compare-runs\Compar_table_temp.xlsx')
		
		



class ComparTable(tk.Frame):
	def __init__(self, parent, rows=6, columns=6):
		# use black background so it "peeks through" to 
		# form grid lines
		tk.Frame.__init__(self, parent, background="black")
		
		self._widgets = []
		for row in range(rows):
			current_row = []
			for column in range(columns):
				label = tk.Label(self, text="%s" % data.ix[row, column +1], 
								 borderwidth=0, width=20)
				label.grid(row=row, column=column, sticky="nsew", padx=1, pady=1)
				current_row.append(label)
			self._widgets.append(current_row)

		for column in range(columns):
			self.grid_columnconfigure(column, weight=1)


	def set(self, row, column, value):
		widget = self._widgets[row][column]
		widget.configure(text=value)

if __name__ == "__main__":
	app = ExampleApp()
	app.mainloop()
