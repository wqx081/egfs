@if exist *.beam del *.beam

@for %%c in (*.erl) do erlc %%c
@for %%c in (*.erl) do echo %%c
@pause
