OBJDIR := ebin
#CMP_FORM_DIR := priv/compiled_layout
#SRC_FORM_DIR := resources/wx

MODULES := csv merge csv_lex data_sink file_chooser merge data_sink line_accumulator file_reader

# empty_panel is used for development purposes.  Currently debugging 
# loading a panel from xrc using wxobject_from_xml
FORMS := 

# The wxobject_from_xml loads an xrc file and creates a wxobject based on its contents
# of course that leaves the actual implementation of callbacks to other modules.  One
# such implementation is control_frame.  This naturally build a dependency between
# the two modules.

DST := $(foreach mod, $(MODULES), $(OBJDIR)/$(mod).beam)
#FORMS := $(foreach form, $(FORMS), $(CMP_FORM_DIR)/$(form).xrc)
#DEMOS := $(foreach mod, $(DEMO_MODS), $(OBJDIR)/$(mod).beam)

#all: $(DST) $(FORMS)
#	echo $(DST)
all: $(DST) 
	echo $(DST)

ebin/%.beam: src/%.erl
	erlc -I include -o $(OBJDIR) $<
