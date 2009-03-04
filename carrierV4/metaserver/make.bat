@if exist *.beam del *.beam
@pause

@for %%c in (*.erl) do erlc %%c
@for %%c in (*.erl) do echo %%c
@pause
