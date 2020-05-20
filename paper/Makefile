DOCNAME=paper

all: paper

.PHONY: clean

paper:
	pdflatex $(DOCNAME).tex
	bibtex $(DOCNAME).aux
	pdflatex $(DOCNAME).tex
	pdflatex $(DOCNAME).tex

view: paper
	evince $(DOCNAME).pdf &

clean:
	rm *.blg *.bbl *.aux *.log
