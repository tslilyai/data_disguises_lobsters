PAPER = paper
TEX = $(wildcard *.tex)
BIB = paper.bib
#FIG = $(shell find figures -name "*")
#GRAPH = $(shell find graphs -name "*.pdf")

.PHONY: all clean

$(PAPER).pdf: $(TEX) $(BIB) #$(FIG) $(GRAPH)
	latexmk -pdf -shell-escape $(PAPER)

clean:
	rm -f *.aux *.bbl *.blg *.log *.out $(PAPER).pdf *.bcf
