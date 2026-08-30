package domain

import "testing"

func TestTokenContainment(t *testing.T) {
	cases := []struct {
		name    string
		tok     Token
		wantErr bool
	}{
		{
			name: "ordered ceilings pass",
			tok:  Token{MaxAmountPaise: 200000, MaxPerDayPaise: 500000, TokenCeilingPaise: 2000000},
		},
		{
			name: "equal ceilings pass (<=, not <)",
			tok:  Token{MaxAmountPaise: 100000, MaxPerDayPaise: 100000, TokenCeilingPaise: 100000},
		},
		{
			name:    "per-debit above per-day is rejected",
			tok:     Token{MaxAmountPaise: 600000, MaxPerDayPaise: 500000, TokenCeilingPaise: 2000000},
			wantErr: true,
		},
		{
			name:    "per-day above lifetime is rejected",
			tok:     Token{MaxAmountPaise: 100000, MaxPerDayPaise: 3000000, TokenCeilingPaise: 2000000},
			wantErr: true,
		},
		{
			name:    "zero ceiling is rejected, never clamped",
			tok:     Token{MaxAmountPaise: 0, MaxPerDayPaise: 500000, TokenCeilingPaise: 2000000},
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.tok.Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() error = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}
