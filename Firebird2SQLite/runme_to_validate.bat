@ECHO OFF
FOR /R %%f IN (*.GDB) DO (
	IF EXIST %%~df%%~pf%%~nf.sql (
		ECHO Processing %%~nf
		python validate_fb2sqlite.py %%f %%~df%%~pf%%~nf.sql 
		)
	)