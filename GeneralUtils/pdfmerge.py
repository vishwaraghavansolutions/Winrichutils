from PyPDF2 import PdfMerger

# List your PDF files in the order you want them merged
# pdf_files = [
#     "Degree Certificate.pdf",
#     "Sem1 marksheet.pdf",
#     "Sem2 marksheet.pdf",
#     "Sem3 marksheet.pdf",
#     "Sem4 marksheet.pdf",
#     "Sem5 marksheet.pdf",
#     "Sem6 marksheet.pdf"
# ]

#List your PDF files in the order you want them merged
# pdf_files = [
#     "msc sem 1.pdf",
#     "msc sem 2.pdf",
#     "msc sem 3.pdf"
# ]

#School marksheet
# pdf_files = [
#     "Bhavya V 10th Marksheet.pdf",
#     "Bhavya V 12th Marksheet.pdf"
# ]

# pdf_files = [
#     "Bhavya_CV.pdf",
#     "Bhavya_Researchsynopsis.pdf"
# ]

# pdf_files = [
#     "Bhavya UoH Recommendation_Ajay.pdf",
#     "Letter of Recommendation_Naresh.pdf"
# ]

# pdf_files = [
#     "IMG_20260411_0001.pdf",
#     "IMG_20260411_0002.pdf",
#     "IMG_20260411_0003.pdf"
# ]


pdf_files = [
    "Bhavya new Passport.pdf",
    "Bhavya OCI Latest.pdf"
]

merger = PdfMerger()

for pdf in pdf_files:
    merger.append(pdf)

merger.write("Bhavya_Passport_OCI.pdf")
merger.close()
